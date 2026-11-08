"""Shared pytest fixtures: isolated local "buckets", config, Spark session, DuckDB."""

from __future__ import annotations

import os
import shutil
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent
# Keep Airflow's metadata/config out of ~/airflow during tests.
os.environ.setdefault("AIRFLOW_HOME", str(ROOT / ".airflow_local"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
for extra in (ROOT, ROOT / "airflow" / "plugins"):
    if str(extra) not in sys.path:
        sys.path.insert(0, str(extra))

from cdc_common.config import PipelineConfig, load_config  # noqa: E402

FIXTURES = ROOT / "glue" / "tests" / "fixtures"


def make_env(tmp: Path, **overrides: str) -> dict[str, str]:
    """Environment for a fully local, isolated configuration."""
    env = {
        "PIPELINE_ENV": "local",
        "RAW_FORMAT": "jsonl",
        "BQ_BACKEND": "duckdb",
        "TRANSFER_MODE": "airflow_worker",
        "RAW_BUCKET_URI": f"file://{tmp / 's3' / 'raw'}",
        "PROCESSED_BUCKET_URI": f"file://{tmp / 's3' / 'processed'}",
        "GCS_STAGING_URI": f"file://{tmp / 'gcs' / 'staging'}",
        "DUCKDB_PATH": str(tmp / "warehouse.duckdb"),
        "CDC_MAX_REJECTED_RATIO": "0.9",
        "CDC_WATERMARK_OVERLAP_MINUTES": "0",
    }
    env.update(overrides)
    return env


@pytest.fixture
def local_cfg(tmp_path: Path) -> PipelineConfig:
    """Pipeline config pointing at temporary local buckets and DuckDB."""
    return load_config(env=make_env(tmp_path))


@pytest.fixture
def default_cfg() -> PipelineConfig:
    """Config with no environment overrides (placeholder names)."""
    return load_config(env={})


def stage_fixture(cfg: PipelineConfig, fixture: str, key: str) -> str:
    """Copy a JSONL fixture into the raw bucket at ``key``; return the key."""
    raw_root = Path(cfg.raw_uri.removeprefix("file://"))
    dest = raw_root / key
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(FIXTURES / fixture, dest)
    return key


@pytest.fixture(scope="session")
def spark() -> Iterator[object]:
    """Local Spark session (skips the test when pyspark / Java are unavailable)."""
    pytest.importorskip("pyspark")
    if shutil.which("java") is None:
        pytest.skip("Java is required for PySpark tests")
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)  # workers = same interpreter
    session = (
        SparkSession.builder.master("local[1]")
        .appName("cdc-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
