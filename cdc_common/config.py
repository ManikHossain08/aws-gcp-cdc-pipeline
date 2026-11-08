"""Typed loader and validator for config/pipeline_config.yaml."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from cdc_common.types import PhysicalTypes, physical_types

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = REPO_ROOT / "config" / "pipeline_config.yaml"

_ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)(?::-([^}]*))?\}")
_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
_DELETE_STRATEGIES = frozenset({"soft", "hard"})


class ConfigError(ValueError):
    """Raised when the pipeline configuration is invalid."""


@dataclass(frozen=True)
class ColumnConfig:
    """A business column of a replicated table."""

    name: str
    logical_type: str
    nullable: bool = True
    pii: bool = False

    @property
    def types(self) -> PhysicalTypes:
        """Engine-specific types for this column."""
        return physical_types(self.logical_type)


@dataclass(frozen=True)
class TableConfig:
    """Contract for one replicated source table."""

    name: str
    source_schema: str
    primary_key: tuple[str, ...]
    staging_table: str
    final_table: str
    columns: tuple[ColumnConfig, ...]
    partition_field: str | None = None
    cluster_by: tuple[str, ...] = ()

    @property
    def column_names(self) -> list[str]:
        """Business column names in contract order."""
        return [c.name for c in self.columns]

    def column(self, name: str) -> ColumnConfig:
        """Return the column called ``name``."""
        for col in self.columns:
            if col.name == name:
                return col
        raise KeyError(name)


@dataclass(frozen=True)
class CdcFields:
    """Names of the DMS-produced CDC metadata columns."""

    operation: str
    cdc_timestamp: str
    ingest_timestamp: str
    sequence: str
    transaction_id: str
    source_table: str


@dataclass(frozen=True)
class PipelineConfig:
    """Fully resolved pipeline configuration."""

    name: str
    environment: str
    delete_strategy: str
    max_rejected_ratio: float
    watermark_prefix: str
    watermark_overlap_minutes: int
    raw_uri: str
    raw_prefix: str
    raw_format: str
    processed_uri: str
    processed_prefixes: dict[str, str]
    gcs_staging_uri: str
    gcs_prefix: str
    transfer_mode: str
    transfer_glue_job_name: str
    gcp_credentials_secret_id: str
    glue_transform_job_name: str
    aws_region: str
    glue_poll_seconds: int
    bq_backend: str
    duckdb_path: str
    bq_project: str
    bq_location: str
    bq_staging_dataset: str
    bq_final_dataset: str
    bq_audit_dataset: str
    bq_audit_table: str
    cdc_fields: CdcFields
    tables: tuple[TableConfig, ...] = field(default_factory=tuple)

    def table(self, name: str) -> TableConfig:
        """Return the table contract called ``name``."""
        for tbl in self.tables:
            if tbl.name == name:
                return tbl
        raise KeyError(f"Table {name!r} is not configured")

    def select_tables(self, names: list[str] | None) -> list[TableConfig]:
        """Configured tables filtered by ``names`` (all when empty)."""
        if not names:
            return list(self.tables)
        return [self.table(n) for n in names]

    def prefix(self, kind: str) -> str:
        """Processed-bucket prefix for ``kind`` (validated, deduplicated, ...)."""
        return self.processed_prefixes[kind]


def resolve_env(value: Any, env: dict[str, str] | None = None) -> Any:
    """Recursively substitute ``${VAR:-default}`` placeholders."""
    env = dict(os.environ) if env is None else env
    if isinstance(value, str):
        return _ENV_PATTERN.sub(lambda m: env.get(m.group(1)) or (m.group(2) or ""), value)
    if isinstance(value, list):
        return [resolve_env(v, env) for v in value]
    if isinstance(value, dict):
        return {k: resolve_env(v, env) for k, v in value.items()}
    return value


def resolve_uri(uri: str) -> str:
    """Make relative ``file://`` URIs absolute against the repository root."""
    if uri.startswith("file://") and not uri.startswith("file:///"):
        return f"file://{(REPO_ROOT / uri[len('file://') :]).resolve()}"
    return uri.rstrip("/")


def load_config(
    path: str | Path | None = None, env: dict[str, str] | None = None
) -> PipelineConfig:
    """Load, resolve and validate the pipeline configuration."""
    config_path = Path(path or os.environ.get("PIPELINE_CONFIG_PATH") or DEFAULT_CONFIG_PATH)
    raw = resolve_env(yaml.safe_load(config_path.read_text(encoding="utf-8")), env)
    return parse_config(raw)


def parse_config(raw: dict[str, Any]) -> PipelineConfig:
    """Validate an already env-resolved configuration mapping."""
    try:
        pipeline, storage, bq = raw["pipeline"], raw["storage"], raw["bigquery"]
        transfer, glue, fields = raw["transfer"], raw["glue"], raw["cdc_fields"]
    except KeyError as exc:
        raise ConfigError(f"Missing config section {exc}") from exc

    delete_strategy = str(pipeline["delete_strategy"]).lower()
    if delete_strategy not in _DELETE_STRATEGIES:
        raise ConfigError(f"delete_strategy must be one of {sorted(_DELETE_STRATEGIES)}")
    if storage["raw_format"] not in {"parquet", "jsonl"}:
        raise ConfigError("storage.raw_format must be parquet or jsonl")
    if bq["backend"] not in {"bigquery", "duckdb"}:
        raise ConfigError("bigquery.backend must be bigquery or duckdb")
    if transfer["mode"] not in {"glue_python_shell", "airflow_worker"}:
        raise ConfigError("transfer.mode must be glue_python_shell or airflow_worker")

    tables = tuple(_parse_table(t) for t in raw.get("tables", []))
    if not tables:
        raise ConfigError("At least one table must be configured")
    names = [t.name for t in tables]
    if len(names) != len(set(names)):
        raise ConfigError(f"Duplicate table names in {names}")

    duckdb_path = str(bq["duckdb_path"])
    if not Path(duckdb_path).is_absolute():
        duckdb_path = str(REPO_ROOT / duckdb_path)

    return PipelineConfig(
        name=pipeline["name"],
        environment=pipeline["environment"],
        delete_strategy=delete_strategy,
        max_rejected_ratio=float(pipeline["max_rejected_ratio"]),
        watermark_prefix=pipeline["watermark"]["prefix"].strip("/"),
        watermark_overlap_minutes=int(pipeline["watermark"]["overlap_minutes"]),
        raw_uri=resolve_uri(storage["raw_uri"]),
        raw_prefix=storage["raw_prefix"].strip("/"),
        raw_format=storage["raw_format"],
        processed_uri=resolve_uri(storage["processed_uri"]),
        processed_prefixes={k: v.strip("/") for k, v in storage["processed_prefixes"].items()},
        gcs_staging_uri=resolve_uri(storage["gcs_staging_uri"]),
        gcs_prefix=storage["gcs_prefix"].strip("/"),
        transfer_mode=transfer["mode"],
        transfer_glue_job_name=transfer["glue_job_name"],
        gcp_credentials_secret_id=transfer.get("gcp_credentials_secret_id") or "",
        glue_transform_job_name=glue["transform_job_name"],
        aws_region=glue["aws_region"],
        glue_poll_seconds=int(glue.get("poll_seconds", 30)),
        bq_backend=bq["backend"],
        duckdb_path=duckdb_path,
        bq_project=bq["project"],
        bq_location=bq["location"],
        bq_staging_dataset=bq["staging_dataset"],
        bq_final_dataset=bq["final_dataset"],
        bq_audit_dataset=bq["audit_dataset"],
        bq_audit_table=bq["audit_table"],
        cdc_fields=CdcFields(**fields),
        tables=tables,
    )


def _parse_table(raw: dict[str, Any]) -> TableConfig:
    name = raw.get("name", "")
    _check_identifier(name, "table")
    columns = tuple(
        ColumnConfig(
            name=c["name"],
            logical_type=str(c["type"]),
            nullable=bool(c.get("nullable", True)),
            pii=bool(c.get("pii", False)),
        )
        for c in raw.get("columns", [])
    )
    col_names = [c.name for c in columns]
    for col in columns:
        _check_identifier(col.name, f"{name} column")
        col.types  # noqa: B018 - validates the logical type eagerly
    if len(col_names) != len(set(col_names)):
        raise ConfigError(f"{name}: duplicate column names")

    pk = tuple(raw.get("primary_key", []))
    if not pk:
        raise ConfigError(f"{name}: primary_key is required for CDC")
    for key in pk:
        if key not in col_names:
            raise ConfigError(f"{name}: primary key {key!r} is not a column")
        if next(c for c in columns if c.name == key).nullable:
            raise ConfigError(f"{name}: primary key {key!r} must be nullable: false")

    partition = raw.get("partition_field")
    if partition is not None:
        if partition not in col_names:
            raise ConfigError(f"{name}: partition_field {partition!r} is not a column")
        ptype = next(c for c in columns if c.name == partition).logical_type
        if ptype not in {"date", "timestamp"}:
            raise ConfigError(f"{name}: partition_field must be date or timestamp")
    cluster = tuple(raw.get("cluster_by", []))
    if len(cluster) > 4 or any(c not in col_names for c in cluster):
        raise ConfigError(f"{name}: cluster_by must be <= 4 existing columns")

    for key in ("staging_table", "final_table"):
        _check_identifier(raw.get(key, ""), f"{name}.{key}")

    return TableConfig(
        name=name,
        source_schema=raw.get("source_schema", "public"),
        primary_key=pk,
        staging_table=raw["staging_table"],
        final_table=raw["final_table"],
        columns=columns,
        partition_field=partition,
        cluster_by=cluster,
    )


def _check_identifier(value: str, kind: str) -> None:
    if not isinstance(value, str) or not _IDENTIFIER.match(value):
        raise ConfigError(
            f"Invalid {kind} name {value!r}: lowercase snake_case; leading '_' is reserved "
            "for pipeline metadata columns"
        )
