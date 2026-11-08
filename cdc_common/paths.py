"""Object-key layout for every zone, in one place.

Raw (written by DMS, see dms/dms_target_endpoint.json):
    cdc/<schema>/<table>/LOAD00000001.parquet                 full load
    cdc/<schema>/<table>/YYYY/MM/DD/<timestamp>.parquet       CDC (DatePartitionEnabled)
  DMS cannot emit Hive-style ``ingest_date=`` folders; the ingest date is derived from the
  date folders (``ingest_date=YYYY-MM-DD`` folders are accepted too).

Processed (written by Glue; Hive-style partitions, run-scoped => idempotent overwrite):
    validated/table=<t>/run_id=<r>/ingest_date=<d>/part-*.parquet
    deduplicated/table=<t>/run_id=<r>/upserts/part-*.parquet
    deduplicated/table=<t>/run_id=<r>/deletes/part-*.parquet
    rejected/table=<t>/run_id=<r>/ingest_date=<d>/part-*.json
    audit/run_id=<r>/table=<t>/glue_manifest.json
    audit/run_id=<r>/table=<t>/transfer_result.json
    control/runs/run_id=<r>/input_manifest.json
    control/watermarks/<t>.json

GCS staging:
    cdc/<t>/run_id=<r>/{upserts,deletes}/part-*.parquet
"""

from __future__ import annotations

import re
from datetime import date, datetime, timezone

from cdc_common.config import PipelineConfig, TableConfig

RUN_ID_PATTERN = re.compile(r"^cdc_\d{8}T\d{6}Z(_[a-z0-9]{1,12})?$")
_DATE_FOLDERS = re.compile(r"/(\d{4})/(\d{2})/(\d{2})/[^/]+$")
_HIVE_DATE = re.compile(r"ingest_date=(\d{4}-\d{2}-\d{2})")
OUTPUT_KINDS = ("upserts", "deletes")


def build_run_id(when: datetime, suffix: str | None = None) -> str:
    """Deterministic, sortable run id, e.g. ``cdc_20261105T083000Z``."""
    run_id = f"cdc_{when.astimezone(timezone.utc):%Y%m%dT%H%M%SZ}"
    if suffix:
        run_id += "_" + re.sub(r"[^a-z0-9]", "", suffix.lower())[:12]
    return run_id


def validate_run_id(run_id: str) -> str:
    """Reject run ids that could break key layouts (path traversal, odd characters)."""
    if not RUN_ID_PATTERN.match(run_id):
        raise ValueError(f"Malformed run_id {run_id!r}; expected cdc_YYYYMMDDTHHMMSSZ[_suffix]")
    return run_id


def ingest_date_from_key(key: str) -> date | None:
    """Ingest date encoded in a raw object key, or None for full-load files."""
    hive = _HIVE_DATE.search(key)
    if hive:
        return date.fromisoformat(hive.group(1))
    folders = _DATE_FOLDERS.search(key)
    if folders:
        return date(int(folders.group(1)), int(folders.group(2)), int(folders.group(3)))
    return None


def is_full_load_key(key: str) -> bool:
    """DMS names full-load files LOAD<8 digits>.<ext>."""
    return key.rsplit("/", 1)[-1].startswith("LOAD")


def raw_table_prefix(cfg: PipelineConfig, table: TableConfig) -> str:
    """Root prefix of one table in the raw bucket."""
    return f"{cfg.raw_prefix}/{table.source_schema}/{table.name}/"


def raw_date_prefix(cfg: PipelineConfig, table: TableConfig, day: date) -> str:
    """Prefix of one ingest date of one table in the raw bucket."""
    return f"{raw_table_prefix(cfg, table)}{day:%Y/%m/%d}/"


def validated_prefix(cfg: PipelineConfig, table: str, run_id: str) -> str:
    """Schema-validated records of a run (partitioned by ingest_date inside)."""
    return f"{cfg.prefix('validated')}/table={table}/run_id={run_id}/"


def deduplicated_prefix(cfg: PipelineConfig, table: str, run_id: str, kind: str) -> str:
    """Latest change per primary key of a run, split into upserts / deletes."""
    if kind not in OUTPUT_KINDS:
        raise ValueError(kind)
    return f"{cfg.prefix('deduplicated')}/table={table}/run_id={run_id}/{kind}/"


def rejected_prefix(cfg: PipelineConfig, table: str, run_id: str) -> str:
    """Records that failed validation, with error metadata."""
    return f"{cfg.prefix('rejected')}/table={table}/run_id={run_id}/"


def glue_manifest_key(cfg: PipelineConfig, table: str, run_id: str) -> str:
    """Audit manifest written by the Glue job."""
    return f"{cfg.prefix('audit')}/run_id={run_id}/table={table}/glue_manifest.json"


def transfer_result_key(cfg: PipelineConfig, table: str, run_id: str) -> str:
    """Transfer success/failure metadata."""
    return f"{cfg.prefix('audit')}/run_id={run_id}/table={table}/transfer_result.json"


def input_manifest_key(cfg: PipelineConfig, run_id: str) -> str:
    """Raw files selected for a run by ``check_new_cdc_files``."""
    return f"{cfg.prefix('control')}/runs/run_id={run_id}/input_manifest.json"


def watermark_key(cfg: PipelineConfig, table: str) -> str:
    """Watermark control file of one table."""
    return f"{cfg.watermark_prefix}/{table}.json"


def gcs_prefix(cfg: PipelineConfig, table: str, run_id: str, kind: str) -> str:
    """GCS staging location of one run's output."""
    return f"{cfg.gcs_prefix}/{table}/run_id={run_id}/{kind}/"
