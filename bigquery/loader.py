"""Staging load, MERGE, delete application and audit writes for one pipeline run."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from bigquery import sql_builder as sb
from bigquery.warehouse import Warehouse
from cdc_common import metadata as m
from cdc_common.config import PipelineConfig, TableConfig
from cdc_common.logging_utils import get_logger
from cdc_common.manifest import GlueAuditManifest
from transfer.s3_to_gcs_transfer import TransferResult

log = get_logger("bigquery.loader")


class StagingCountMismatchError(RuntimeError):
    """Staged rows differ from the rows the Glue manifest promised."""


def load_staging(
    cfg: PipelineConfig,
    wh: Warehouse,
    table: TableConfig,
    transfer: TransferResult,
    manifest: GlueAuditManifest,
) -> int:
    """Replace the staging table with exactly the run's transferred files and verify counts."""
    uris = transfer.uris("upserts") + transfer.uris("deletes")
    wh.load_parquet(uris, cfg.bq_staging_dataset, table.staging_table)
    stage = wh.dialect.table(cfg.bq_project, cfg.bq_staging_dataset, table.staging_table)
    rows, _ = wh.execute(
        f"SELECT COUNT(*) AS n FROM {stage} "  # noqa: S608 - identifiers are validated
        f"WHERE {wh.dialect.ident(m.PIPELINE_RUN_ID)} = {wh.dialect.param(sb.RUN_ID_PARAM)}",
        {sb.RUN_ID_PARAM: manifest.pipeline_run_id},
    )
    staged = int(rows[0]["n"])
    expected = manifest.upsert_row_count + manifest.delete_row_count
    if staged != expected:
        raise StagingCountMismatchError(
            f"{table.name}: staged {staged} rows, manifest expects {expected} "
            f"(run {manifest.pipeline_run_id})"
        )
    log.info(
        "staging_loaded",
        extra={"event": "staging_loaded", "table": table.name, "files": len(uris), "rows": staged},
    )
    return staged


def merge_upserts(cfg: PipelineConfig, wh: Warehouse, table: TableConfig, run_id: str) -> int:
    """MERGE INSERT/UPDATE changes; returns rows inserted or updated."""
    _, affected = wh.execute(
        sb.build_merge_upserts(cfg, table, wh.dialect), {sb.RUN_ID_PARAM: run_id}
    )
    log.info(
        "upserts_merged", extra={"event": "upserts_merged", "table": table.name, "rows": affected}
    )
    return affected


def apply_deletes(
    cfg: PipelineConfig, wh: Warehouse, table: TableConfig, run_id: str, strategy: str
) -> int:
    """Apply DELETE changes with the soft/hard strategy; returns rows affected."""
    _, affected = wh.execute(
        sb.build_apply_deletes(cfg, table, strategy, wh.dialect), {sb.RUN_ID_PARAM: run_id}
    )
    log.info(
        "deletes_applied",
        extra={
            "event": "deletes_applied",
            "table": table.name,
            "strategy": strategy,
            "rows": affected,
        },
    )
    return affected


def insert_audit_row(cfg: PipelineConfig, wh: Warehouse, row: dict[str, Any]) -> None:
    """Append one audit row; missing fields are written as NULL."""
    params = {name: row.get(name) for name, _ in sb.AUDIT_COLUMNS}
    for key in ("watermark", "started_at", "finished_at"):
        value = params[key]
        if isinstance(value, str):
            params[key] = datetime.fromisoformat(value)
    wh.execute(sb.build_audit_insert(cfg, wh.dialect), params)


def deploy_ddl(cfg: PipelineConfig, wh: Warehouse) -> int:
    """Create datasets and tables if missing (idempotent). Returns statements executed."""
    stmts = sb.build_all_ddl(cfg, wh.dialect)
    for stmt in stmts:
        wh.execute(stmt)
    return len(stmts)
