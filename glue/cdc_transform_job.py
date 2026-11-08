"""AWS Glue 5.0 (Spark 3.5, Python 3.11) CDC transformation job.

For every configured table with input files in the run's input manifest:

  raw DMS files --read (explicit schema)--> validate --+--> rejected/   (errors + raw record)
                                                       +--> validated/  (typed, all changes)
                                                              |
                                              latest change per PK (dedup)
                                                       +--> deduplicated/.../upserts/
                                                       +--> deduplicated/.../deletes/
                                                       +--> audit/.../glue_manifest.json  (last)

The run's input files are chosen by Airflow (``check_new_cdc_files``) and passed via an
input manifest, so re-running a run id processes exactly the same files and overwrites the
same run-scoped output prefixes (idempotent).

Glue job arguments:
  --run_id               pipeline run id (cdc_YYYYMMDDTHHMMSSZ)
  --config_s3_uri        s3://.../pipeline_config.yaml
  --config_env_json      JSON object of env overrides for ${VAR} placeholders (bucket URIs...)
  --rows_per_file        optional, default 500000
  --tables               optional comma list (default: tables present in the input manifest)

Locally the same ``run_transform`` is called with a local SparkSession
(scripts/run_local_pipeline.py, Airflow local mode).
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from cdc_common import metadata as m
from cdc_common import paths
from cdc_common.config import PipelineConfig, TableConfig, load_config
from cdc_common.logging_utils import get_logger, set_run_id
from cdc_common.manifest import GlueAuditManifest
from cdc_common.storage import ObjectStore, read_json, store_for_uri
from glue.utils import validators
from glue.utils.audit import utc_now_iso, write_manifest
from glue.utils.deduplication import latest_per_key, split_upserts_deletes
from glue.utils.s3_io import describe_outputs, read_raw, write_json, write_parquet

log = get_logger("cdc_transform_job")
DEFAULT_ROWS_PER_FILE = 500_000


def transform_table(
    spark: SparkSession,
    cfg: PipelineConfig,
    store: ObjectStore,
    table: TableConfig,
    run_id: str,
    input_keys: list[str],
    rows_per_file: int = DEFAULT_ROWS_PER_FILE,
) -> GlueAuditManifest:
    """Process one table's input files for ``run_id`` and write all zones + manifest."""
    manifest = GlueAuditManifest(
        pipeline_run_id=run_id,
        table=table.name,
        status="SUCCEEDED",
        input_file_count=len(input_keys),
        input_files=input_keys,
        processing_started_at=utc_now_iso(),
    )
    ctx = {"table": table.name, "input_files": len(input_keys)}
    if input_keys:
        raw = read_raw(spark, cfg, table, input_keys)
        checked = validators.validate(raw, table, cfg.cdc_fields).cache()

        valid = validators.valid_records(checked, table, cfg.cdc_fields, run_id).cache()
        rejected = validators.rejected_records(checked, table, cfg.cdc_fields, run_id)
        latest = latest_per_key(valid, table).cache()
        upserts, deletes = split_upserts_deletes(latest)

        manifest.input_row_count = checked.count()
        manifest.valid_row_count = valid.count()
        manifest.rejected_row_count = manifest.input_row_count - manifest.valid_row_count
        op_counts = {
            r[m.CDC_OPERATION]: int(r["count"])
            for r in latest.groupBy(m.CDC_OPERATION).count().collect()
        }
        manifest.delete_row_count = op_counts.get(m.OP_DELETE, 0)
        manifest.upsert_row_count = sum(op_counts.get(op, 0) for op in m.UPSERT_OPS)
        manifest.duplicate_row_count = manifest.valid_row_count - (
            manifest.upsert_row_count + manifest.delete_row_count
        )

        write_parquet(
            valid,
            cfg,
            paths.validated_prefix(cfg, table.name, run_id),
            manifest.valid_row_count,
            rows_per_file,
            partition_by=[m.INGEST_DATE],
        )
        write_parquet(
            upserts,
            cfg,
            paths.deduplicated_prefix(cfg, table.name, run_id, "upserts"),
            manifest.upsert_row_count,
            rows_per_file,
        )
        write_parquet(
            deletes,
            cfg,
            paths.deduplicated_prefix(cfg, table.name, run_id, "deletes"),
            manifest.delete_row_count,
            rows_per_file,
        )
        manifest.rejected_prefix = paths.rejected_prefix(cfg, table.name, run_id)
        write_json(
            rejected.withColumn(m.INGEST_DATE, F.col(m.INGEST_DATE).cast("string")),
            cfg,
            manifest.rejected_prefix,
            partition_by=[m.INGEST_DATE],
        )
        if manifest.rejected_row_count:
            log.warning(
                "records_rejected",
                extra={
                    **ctx,
                    "event": "records_rejected",
                    "rejected": manifest.rejected_row_count,
                    "prefix": manifest.rejected_prefix,
                },
            )
        for df in (latest, valid, checked, raw):
            df.unpersist()

    manifest.outputs = {
        kind: describe_outputs(store, paths.deduplicated_prefix(cfg, table.name, run_id, kind))
        for kind in paths.OUTPUT_KINDS
    }
    manifest.processing_finished_at = utc_now_iso()
    problems = manifest.check_consistency()
    if problems:
        raise RuntimeError(f"{table.name}: manifest inconsistent: {problems}")
    write_manifest(cfg, store, manifest)
    log.info(
        "table_transformed",
        extra={
            **ctx,
            "event": "table_transformed",
            **{k: v for k, v in manifest.to_dict().items() if k.endswith("_count")},
        },
    )
    return manifest


def run_transform(
    spark: SparkSession,
    cfg: PipelineConfig,
    run_id: str,
    tables: list[str] | None = None,
    rows_per_file: int = DEFAULT_ROWS_PER_FILE,
) -> list[GlueAuditManifest]:
    """Transform every table listed in the run's input manifest."""
    paths.validate_run_id(run_id)
    set_run_id(run_id)
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    store = store_for_uri(cfg.processed_uri)
    input_manifest: dict[str, Any] = read_json(store, paths.input_manifest_key(cfg, run_id))

    results = []
    for table_name, files in input_manifest["tables"].items():
        if tables and table_name not in tables:
            continue
        keys = [f["key"] for f in files]
        try:
            results.append(
                transform_table(
                    spark, cfg, store, cfg.table(table_name), run_id, keys, rows_per_file
                )
            )
        except Exception as exc:
            failed = GlueAuditManifest(
                pipeline_run_id=run_id,
                table=table_name,
                status="FAILED",
                input_file_count=len(keys),
                input_files=keys,
                error=f"{type(exc).__name__}: {exc}"[:2000],
                processing_finished_at=utc_now_iso(),
            )
            write_manifest(cfg, store, failed)
            log.exception("table_failed", extra={"event": "table_failed", "table": table_name})
            raise
    return results


def _glue_main() -> None:  # pragma: no cover - exercised only inside AWS Glue
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions

    optional = [a for a in ("rows_per_file", "tables", "config_env_json") if f"--{a}" in sys.argv]
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "run_id", "config_s3_uri", *optional])
    glue_ctx = GlueContext(SparkSession.builder.getOrCreate().sparkContext)
    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    bucket_uri, _, key = args["config_s3_uri"].removeprefix("s3://").partition("/")
    with tempfile.TemporaryDirectory() as tmp:
        cfg_path = Path(tmp) / "pipeline_config.yaml"
        cfg_path.write_bytes(store_for_uri(f"s3://{bucket_uri}").read_bytes(key))
        env = json.loads(args.get("config_env_json") or "{}")
        cfg = load_config(cfg_path, env={**os.environ, **env})

    tables = [t for t in args.get("tables", "").split(",") if t] or None
    run_transform(
        glue_ctx.spark_session,
        cfg,
        args["run_id"],
        tables,
        int(args.get("rows_per_file", DEFAULT_ROWS_PER_FILE)),
    )
    job.commit()


if __name__ == "__main__":
    _glue_main()
