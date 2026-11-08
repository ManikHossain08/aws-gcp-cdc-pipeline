"""Run the whole pipeline locally, task by task, exactly as the Airflow DAG would.

Local stand-ins (no cloud account needed):
  S3 raw/processed  -> local_data/s3/<bucket>/      (filesystem ObjectStore)
  AWS DMS           -> scripts/generate_test_cdc_events.py (DMS-shaped JSONL)
  AWS Glue          -> the real Glue job code on local PySpark
  GCS staging       -> local_data/gcs/<bucket>/
  BigQuery          -> DuckDB executing the same MERGE/DELETE SQL (DuckDB dialect)

Usage:
  python scripts/run_local_pipeline.py --reset --batches 2 --rerun
  python scripts/run_local_pipeline.py --batches 1 --delete-strategy hard
  python scripts/run_local_pipeline.py --reset --source postgres --batches 2   # needs Docker
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "airflow" / "plugins")]
os.environ.setdefault("PIPELINE_ENV", "local")
os.environ.setdefault("BQ_BACKEND", "duckdb")
os.environ.setdefault("RAW_FORMAT", "jsonl")
os.environ.setdefault("TRANSFER_MODE", "airflow_worker")
# The demo data deliberately contains malformed records.
os.environ.setdefault("CDC_MAX_REJECTED_RATIO", "0.5")

import cdc_operators as ops  # noqa: E402
from bigquery.loader import deploy_ddl  # noqa: E402
from bigquery.warehouse import DuckDBWarehouse  # noqa: E402
from cdc_common.logging_utils import get_logger  # noqa: E402
from cdc_common.paths import build_run_id  # noqa: E402
from scripts import generate_test_cdc_events as gen  # noqa: E402
from scripts.verify_bigquery_results import verify  # noqa: E402

log = get_logger("run_local_pipeline")


def run_pipeline(run_id: str, strategy: str | None) -> dict:
    """Execute the nine DAG tasks in order for ``run_id``."""
    cfg = ops.get_config({"CDC_DELETE_STRATEGY": strategy or ""})
    started = datetime.now(timezone.utc).isoformat()
    results: dict = {}
    status, error = "SUCCEEDED", None
    try:
        check = results["check"] = ops.check_new_cdc_files(cfg, run_id)
        if not check["has_data"]:
            status = "NO_DATA"
        else:
            tables = list(check["files"])
            ops.run_glue_cdc_job(cfg, run_id, tables)
            ops.validate_glue_output(cfg, run_id, tables)
            ops.transfer_s3_to_gcs(cfg, run_id, tables)
            results["staged"] = ops.load_gcs_to_bigquery_staging(cfg, run_id, tables)
            results["merged"] = ops.merge_staging_to_final(cfg, run_id, tables)
            results["deleted"] = ops.apply_deletes(cfg, run_id, tables)
            results["watermarks"] = ops.update_watermark(cfg, run_id)
    except Exception as exc:
        status, error = "FAILED", f"{type(exc).__name__}: {exc}"
        raise
    finally:
        ops.publish_audit_metrics(cfg, run_id, f"local__{run_id}", status, started, results, error)
    return results


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--batches", type=int, default=2)
    parser.add_argument("--reset", action="store_true", help="delete local_data first")
    parser.add_argument(
        "--rerun", action="store_true", help="re-run every run id a second time (idempotency proof)"
    )
    parser.add_argument("--delete-strategy", choices=["soft", "hard"], default=None)
    parser.add_argument(
        "--source",
        choices=["synthetic", "postgres"],
        default="synthetic",
        help="postgres = real DML on the local Docker PostgreSQL",
    )
    args = parser.parse_args()

    if args.reset and (ROOT / "local_data").exists():
        shutil.rmtree(ROOT / "local_data")
    cfg = ops.get_config({"CDC_DELETE_STRATEGY": args.delete_strategy or ""})
    wh = DuckDBWarehouse(cfg.duckdb_path)
    deploy_ddl(cfg, wh)
    wh.close()

    raw_store = gen.store_for_uri(cfg.raw_uri)
    base = datetime(2026, 11, 5, 9, 0, tzinfo=timezone.utc)
    for batch in range(1, args.batches + 1):
        (gen.synthetic if args.source == "synthetic" else gen.from_postgres)(cfg, raw_store, batch)
        run_id = build_run_id(base + timedelta(hours=batch))
        print(f"\n=== batch {batch}: {run_id} ===")  # noqa: T201
        print(run_pipeline(run_id, args.delete_strategy))  # noqa: T201
        if args.rerun:
            print(f"--- re-running {run_id} (must not change results) ---")  # noqa: T201
            run_pipeline(run_id, args.delete_strategy)

    ok = verify(ops.get_config({"CDC_DELETE_STRATEGY": args.delete_strategy or ""}))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
