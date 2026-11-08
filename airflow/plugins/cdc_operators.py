"""Task implementations for the ``aws_rds_cdc_to_bigquery`` DAG.

Each public function is one DAG task. They are plain Python (no Airflow imports) so they
can be unit-tested and reused by ``scripts/run_local_pipeline.py``. Environment selection:

* ``PIPELINE_ENV=local``: filesystem buckets, local PySpark for the Glue job, DuckDB for
  BigQuery — the whole DAG runs on a laptop with ``airflow dags test``.
* ``PIPELINE_ENV=cloud``: S3 / GCS / BigQuery; Glue jobs are started with boto3 and polled.

Every task is idempotent for a given ``run_id`` (see ARCHITECTURE.md "Idempotency").
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from bigquery import loader  # noqa: E402
from bigquery.warehouse import warehouse_for  # noqa: E402
from cdc_common import paths  # noqa: E402
from cdc_common.config import PipelineConfig, load_config  # noqa: E402
from cdc_common.logging_utils import get_logger, set_run_id  # noqa: E402
from cdc_common.manifest import GlueAuditManifest  # noqa: E402
from cdc_common.storage import read_json, store_for_uri, write_json  # noqa: E402
from cdc_common.watermark import advance_watermark, discover_files, read_watermark  # noqa: E402
from glue.utils.audit import read_manifest  # noqa: E402
from transfer.s3_to_gcs_transfer import read_transfer_result, transfer_run  # noqa: E402

log = get_logger("cdc_operators")


class GlueOutputValidationError(RuntimeError):
    """The Glue audit manifests failed validation."""


def get_config(overrides: dict[str, str] | None = None) -> PipelineConfig:
    """Load the pipeline config, applying task-level env overrides (e.g. delete strategy)."""
    env = {**os.environ, **{k: v for k, v in (overrides or {}).items() if v}}
    return load_config(env=env)


# ----------------------------------------------------------------------------- 1
def check_new_cdc_files(
    cfg: PipelineConfig,
    run_id: str,
    tables: list[str] | None = None,
    ingest_date: str | None = None,
) -> dict[str, Any]:
    """Select raw files for the run and persist them as the run's input manifest.

    Re-running an existing run id reuses its saved input manifest (exact replay).
    """
    paths.validate_run_id(run_id)
    set_run_id(run_id)
    processed = store_for_uri(cfg.processed_uri)
    key = paths.input_manifest_key(cfg, run_id)
    if processed.exists(key):
        manifest = read_json(processed, key)
        log.info("input_manifest_reused", extra={"event": "input_manifest_reused", "key": key})
    else:
        raw = store_for_uri(cfg.raw_uri)
        backfill_day = date.fromisoformat(ingest_date) if ingest_date else None
        selected: dict[str, list[dict[str, Any]]] = {}
        watermarks: dict[str, str] = {}
        for table in cfg.select_tables(tables):
            wm = read_watermark(cfg, processed, table.name)
            watermarks[table.name] = wm.last_modified.isoformat()
            files = discover_files(cfg, raw, table, wm, backfill_day)
            if files:
                selected[table.name] = [
                    {"key": f.key, "size": f.size, "last_modified": f.last_modified.isoformat()}
                    for f in files
                ]
        manifest = {
            "run_id": run_id,
            "mode": "backfill" if backfill_day else "incremental",
            "ingest_date": ingest_date,
            "created_at": _now(),
            "watermarks_before": watermarks,
            "tables": selected,
        }
        write_json(processed, key, manifest)
    counts = {t: len(f) for t, f in manifest["tables"].items()}
    log.info(
        "cdc_files_checked",
        extra={"event": "cdc_files_checked", "files": counts, "mode": manifest["mode"]},
    )
    return {"run_id": run_id, "files": counts, "has_data": bool(counts), "mode": manifest["mode"]}


# ----------------------------------------------------------------------------- 2
def run_glue_cdc_job(cfg: PipelineConfig, run_id: str, tables: list[str]) -> dict[str, Any]:
    """Run the Glue PySpark transform (local Spark, or AWS Glue via boto3)."""
    set_run_id(run_id)
    if cfg.environment == "local":
        from glue.cdc_transform_job import run_transform

        results = run_transform(_local_spark(), cfg, run_id, tables)
        return {"run_id": run_id, "tables": [r.table for r in results], "glue_run_id": "local"}
    arguments = {"--run_id": run_id, "--tables": ",".join(tables)}
    glue_run_id = _run_glue_job(cfg, cfg.glue_transform_job_name, arguments)
    return {"run_id": run_id, "tables": tables, "glue_run_id": glue_run_id}


# ----------------------------------------------------------------------------- 3
def validate_glue_output(cfg: PipelineConfig, run_id: str, tables: list[str]) -> dict[str, Any]:
    """Every table with input must have a consistent, SUCCEEDED manifest under the
    rejection threshold."""
    store = store_for_uri(cfg.processed_uri)
    problems: list[str] = []
    summary: dict[str, dict[str, int]] = {}
    for table in tables:
        manifest = read_manifest(cfg, store, table, run_id)
        if manifest is None:
            problems.append(f"{table}: no Glue manifest for run {run_id}")
            continue
        if manifest.status != "SUCCEEDED":
            problems.append(f"{table}: Glue status {manifest.status}: {manifest.error}")
        problems += [f"{table}: {p}" for p in manifest.check_consistency()]
        if manifest.rejected_ratio > cfg.max_rejected_ratio:
            problems.append(
                f"{table}: rejected ratio {manifest.rejected_ratio:.2%} > "
                f"{cfg.max_rejected_ratio:.2%} (see {manifest.rejected_prefix})"
            )
        summary[table] = {
            k: v
            for k, v in manifest.to_dict().items()
            if k.endswith("_count") and isinstance(v, int)
        }
    if problems:
        raise GlueOutputValidationError("; ".join(problems))
    log.info("glue_output_valid", extra={"event": "glue_output_valid", "summary": summary})
    return {"run_id": run_id, "tables": summary}


# ----------------------------------------------------------------------------- 4
def transfer_s3_to_gcs(cfg: PipelineConfig, run_id: str, tables: list[str]) -> dict[str, Any]:
    """Copy manifest-listed curated files to GCS (Glue Python Shell or in-worker)."""
    set_run_id(run_id)
    if cfg.transfer_mode == "glue_python_shell" and cfg.environment != "local":
        _run_glue_job(cfg, cfg.transfer_glue_job_name, {"--run_id": run_id})
    else:
        from transfer.config import build_stores

        src, dest = build_stores(cfg)
        transfer_run(cfg, run_id, src, dest, tables)
    store = store_for_uri(cfg.processed_uri)
    out = {}
    for table in tables:
        result = read_transfer_result(cfg, store, table, run_id)
        if result is None or result.status != "SUCCEEDED":
            raise RuntimeError(f"{table}: transfer did not succeed for run {run_id}")
        out[table] = {"files": len(result.files), "bytes_copied": result.bytes_copied}
    return {"run_id": run_id, "tables": out}


# ----------------------------------------------------------------------------- 5-7
def load_gcs_to_bigquery_staging(
    cfg: PipelineConfig, run_id: str, tables: list[str]
) -> dict[str, int]:
    """Load each table's transferred files into its staging table (truncate + verify)."""
    store, wh = store_for_uri(cfg.processed_uri), warehouse_for(cfg)
    try:
        staged = {}
        for name in tables:
            manifest = _manifest(cfg, store, name, run_id)
            transfer = read_transfer_result(cfg, store, name, run_id)
            if transfer is None:
                raise RuntimeError(f"{name}: no transfer result for run {run_id}")
            staged[name] = loader.load_staging(cfg, wh, cfg.table(name), transfer, manifest)
        return staged
    finally:
        _close(wh)


def merge_staging_to_final(cfg: PipelineConfig, run_id: str, tables: list[str]) -> dict[str, int]:
    """MERGE INSERT/UPDATE changes into the final tables."""
    wh = warehouse_for(cfg)
    try:
        return {t: loader.merge_upserts(cfg, wh, cfg.table(t), run_id) for t in tables}
    finally:
        _close(wh)


def apply_deletes(cfg: PipelineConfig, run_id: str, tables: list[str]) -> dict[str, int]:
    """Apply DELETE changes with the configured strategy (soft by default)."""
    wh = warehouse_for(cfg)
    try:
        return {
            t: loader.apply_deletes(cfg, wh, cfg.table(t), run_id, cfg.delete_strategy)
            for t in tables
        }
    finally:
        _close(wh)


# ----------------------------------------------------------------------------- 8
def update_watermark(cfg: PipelineConfig, run_id: str) -> dict[str, str]:
    """Advance each table's watermark to the newest input file of the run (monotonic)."""
    store = store_for_uri(cfg.processed_uri)
    manifest = read_json(store, paths.input_manifest_key(cfg, run_id))
    out = {}
    for table, files in manifest["tables"].items():
        newest = max(datetime.fromisoformat(f["last_modified"]) for f in files)
        out[table] = advance_watermark(cfg, store, table, newest, run_id).last_modified.isoformat()
    log.info("watermarks_updated", extra={"event": "watermarks_updated", "watermarks": out})
    return out


# ----------------------------------------------------------------------------- 9
def publish_audit_metrics(
    cfg: PipelineConfig,
    run_id: str,
    dag_run_id: str | None,
    status: str,
    started_at: str,
    task_results: dict[str, Any],
    error_message: str | None = None,
) -> list[dict[str, Any]]:
    """Write one audit row per table to BigQuery and send an optional Slack summary."""
    store = store_for_uri(cfg.processed_uri)
    finished = datetime.now(timezone.utc)
    started = datetime.fromisoformat(started_at)
    files = (task_results.get("check") or {}).get("files", {})
    rows = []
    for table in files or {t.name: 0 for t in cfg.tables}:
        manifest = read_manifest(cfg, store, table, run_id)
        transfer = read_transfer_result(cfg, store, table, run_id)
        wm = read_watermark(cfg, store, table)
        row = {
            "pipeline_run_id": run_id,
            "dag_run_id": dag_run_id,
            "table_name": table,
            "status": status if files else "NO_DATA",
            "input_file_count": files.get(table, 0),
            "transferred_file_count": len(transfer.files) if transfer else None,
            "staged_row_count": (task_results.get("staged") or {}).get(table),
            "merged_row_count": (task_results.get("merged") or {}).get(table),
            "deletes_applied": (task_results.get("deleted") or {}).get(table),
            "delete_strategy": cfg.delete_strategy,
            "watermark": wm.last_modified,
            "started_at": started,
            "finished_at": finished,
            "duration_seconds": (finished - started).total_seconds(),
            "error_message": error_message,
        }
        if manifest:
            row.update(
                {
                    k: getattr(manifest, k)
                    for k in (
                        "input_row_count",
                        "valid_row_count",
                        "duplicate_row_count",
                        "upsert_row_count",
                        "delete_row_count",
                        "rejected_row_count",
                    )
                }
            )
        rows.append(row)
    wh = warehouse_for(cfg)
    try:
        for row in rows:
            loader.insert_audit_row(cfg, wh, row)
    finally:
        _close(wh)
    log.info(
        "audit_published", extra={"event": "audit_published", "status": status, "rows": len(rows)}
    )
    if status != "SUCCEEDED" or os.environ.get("SLACK_NOTIFY_ON_SUCCESS") == "true":
        notify_slack(
            f"[{cfg.name}] run {run_id}: {status}"
            + (f" — {error_message}" if error_message else "")
        )
    return rows


# ----------------------------------------------------------------------------- alerts
def notify_slack(text: str, webhook_url: str | None = None) -> bool:
    """POST to a Slack incoming webhook if one is configured (SLACK_WEBHOOK_URL)."""
    url = webhook_url or os.environ.get("SLACK_WEBHOOK_URL")
    if not url:
        return False
    if not url.startswith("https://"):
        raise ValueError("SLACK_WEBHOOK_URL must be an https:// URL")
    body = json.dumps({"text": text}).encode()
    headers = {"Content-Type": "application/json"}
    req = urllib.request.Request(url, data=body, headers=headers)  # noqa: S310 - https enforced
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:  # noqa: S310 - configured URL
            return 200 <= resp.status < 300
    except OSError:
        log.exception("slack_notify_failed", extra={"event": "slack_notify_failed"})
        return False


# ----------------------------------------------------------------------------- helpers
def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _manifest(cfg: PipelineConfig, store: Any, table: str, run_id: str) -> GlueAuditManifest:
    manifest = read_manifest(cfg, store, table, run_id)
    if manifest is None:
        raise RuntimeError(f"{table}: no Glue manifest for run {run_id}")
    return manifest


def _close(wh: Any) -> None:
    close = getattr(wh, "close", None)
    if close:
        close()


def _local_spark() -> Any:
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)  # workers = same interpreter
    return (
        SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[2]"))
        .appName("cdc-transform-local")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def _run_glue_job(
    cfg: PipelineConfig,
    job_name: str,
    arguments: dict[str, str],
    glue_client: Any | None = None,
    sleep: Any = time.sleep,
) -> str:
    """Start a Glue job run and poll until it finishes; raise on failure."""
    import boto3

    glue = glue_client or boto3.client("glue", region_name=cfg.aws_region)
    run_id = glue.start_job_run(JobName=job_name, Arguments=arguments)["JobRunId"]
    log.info(
        "glue_job_started",
        extra={"event": "glue_job_started", "job": job_name, "glue_run_id": run_id},
    )
    while True:
        state = glue.get_job_run(JobName=job_name, RunId=run_id)["JobRun"]
        status = state["JobRunState"]
        if status == "SUCCEEDED":
            return str(run_id)
        if status in {"FAILED", "ERROR", "TIMEOUT", "STOPPED"}:
            raise RuntimeError(
                f"Glue job {job_name} run {run_id} {status}: {state.get('ErrorMessage')}"
            )
        sleep(cfg.glue_poll_seconds)
