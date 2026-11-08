"""Controlled S3 -> GCS transfer of one pipeline run's curated output.

Guarantees:
* Only files listed in the Glue audit manifest of the run are copied (never rejected/,
  never other runs, never whole prefixes).
* Every file is MD5-verified twice: against the manifest after download, and by GCS on
  upload (the write fails if the bytes differ).
* Idempotent: a destination object with the same MD5 is skipped, so retries are cheap.
* A ``transfer_result.json`` per table records success/failure for audit and for the
  BigQuery load, which loads exactly the files listed there.

Runs as a Glue Python Shell job (preferred) or inside an Airflow task (alternative).
CLI: python -m transfer.s3_to_gcs_transfer --run_id cdc_20261105T090000Z [--tables orders]
"""

from __future__ import annotations

import argparse
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from cdc_common import paths
from cdc_common.config import PipelineConfig, load_config
from cdc_common.logging_utils import get_logger, set_run_id
from cdc_common.manifest import GlueAuditManifest, OutputFile
from cdc_common.storage import ObjectStore, md5_file, read_json, write_json

log = get_logger("transfer")


class ChecksumMismatchError(RuntimeError):
    """A file's bytes do not match the MD5 recorded in the Glue manifest."""


@dataclass
class TransferredFile:
    """Outcome for one file."""

    kind: str
    source_key: str
    dest_uri: str
    size: int
    md5: str
    action: str  # copied | skipped_identical


@dataclass
class TransferResult:
    """Per-table transfer outcome (persisted as transfer_result.json)."""

    pipeline_run_id: str
    table: str
    status: str = "SUCCEEDED"
    files: list[TransferredFile] = field(default_factory=list)
    bytes_copied: int = 0
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """JSON-serialisable form."""
        return asdict(self)

    def uris(self, kind: str) -> list[str]:
        """Destination URIs of ``kind`` (upserts / deletes) for the BigQuery load."""
        return [f.dest_uri for f in self.files if f.kind == kind]


def copy_file(src: ObjectStore, dest: ObjectStore, file: OutputFile, dest_key: str) -> str:
    """Copy one file with MD5 verification; returns the action taken."""
    if dest.exists(dest_key) and dest.md5(dest_key) == file.md5:
        return "skipped_identical"
    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / Path(file.key).name
        src.download_file(file.key, local)
        actual = md5_file(local)
        if actual != file.md5:
            raise ChecksumMismatchError(
                f"{file.key}: md5 {actual} != manifest {file.md5} (file changed after Glue?)"
            )
        dest.upload_file(local, dest_key, md5_hex=file.md5)
    return "copied"


def transfer_table(
    cfg: PipelineConfig, src: ObjectStore, dest: ObjectStore, manifest: GlueAuditManifest
) -> TransferResult:
    """Transfer every curated file of one table's manifest."""
    result = TransferResult(manifest.pipeline_run_id, manifest.table)
    try:
        if manifest.status != "SUCCEEDED":
            raise RuntimeError(f"Glue manifest status is {manifest.status}; refusing to transfer")
        for kind in paths.OUTPUT_KINDS:
            prefix = paths.gcs_prefix(cfg, manifest.table, manifest.pipeline_run_id, kind)
            for file in manifest.outputs.get(kind, []):
                dest_key = prefix + Path(file.key).name
                action = copy_file(src, dest, file, dest_key)
                result.files.append(
                    TransferredFile(
                        kind, file.key, f"{dest.uri}/{dest_key}", file.size, file.md5, action
                    )
                )
                if action == "copied":
                    result.bytes_copied += file.size
    except Exception as exc:
        result.status, result.error = "FAILED", f"{type(exc).__name__}: {exc}"[:2000]
        raise
    finally:
        write_json(
            src,
            paths.transfer_result_key(cfg, manifest.table, manifest.pipeline_run_id),
            result.to_dict(),
        )
        log.info(
            "table_transferred",
            extra={
                "event": "table_transferred",
                "table": manifest.table,
                "status": result.status,
                "files": len(result.files),
                "bytes_copied": result.bytes_copied,
                "skipped": sum(f.action == "skipped_identical" for f in result.files),
            },
        )
    return result


def transfer_run(
    cfg: PipelineConfig,
    run_id: str,
    src: ObjectStore,
    dest: ObjectStore,
    tables: list[str] | None = None,
) -> list[TransferResult]:
    """Transfer all tables of ``run_id`` that have a SUCCEEDED Glue manifest."""
    paths.validate_run_id(run_id)
    set_run_id(run_id)
    results = []
    for table in cfg.select_tables(tables):
        key = paths.glue_manifest_key(cfg, table.name, run_id)
        if not src.exists(key):
            continue  # table had no input in this run
        manifest = GlueAuditManifest.from_dict(read_json(src, key))
        results.append(transfer_table(cfg, src, dest, manifest))
    return results


def read_transfer_result(
    cfg: PipelineConfig, store: ObjectStore, table: str, run_id: str
) -> TransferResult | None:
    """Load a persisted transfer result."""
    key = paths.transfer_result_key(cfg, table, run_id)
    if not store.exists(key):
        return None
    raw = read_json(store, key)
    raw["files"] = [TransferredFile(**f) for f in raw["files"]]
    return TransferResult(**raw)


def main() -> None:
    """CLI / Glue Python Shell entry point."""
    import json
    import os
    import sys

    from transfer.config import build_stores

    if "--JOB_NAME" in sys.argv:  # running inside Glue Python Shell
        from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]

        args = getResolvedOptions(sys.argv, ["run_id", "config_s3_uri", "config_env_json"])
        from cdc_common.storage import store_for_uri

        bucket, _, key = args["config_s3_uri"].removeprefix("s3://").partition("/")
        cfg_path = Path(tempfile.mkdtemp()) / "pipeline_config.yaml"
        cfg_path.write_bytes(store_for_uri(f"s3://{bucket}").read_bytes(key))
        cfg = load_config(cfg_path, env={**os.environ, **json.loads(args["config_env_json"])})
        run_id, tables = args["run_id"], None
    else:
        parser = argparse.ArgumentParser()
        parser.add_argument("--run_id", required=True)
        parser.add_argument("--tables", default="")
        ns = parser.parse_args()
        cfg, run_id = load_config(), ns.run_id
        tables = [t for t in ns.tables.split(",") if t] or None

    src, dest = build_stores(cfg)
    transfer_run(cfg, run_id, src, dest, tables)


if __name__ == "__main__":
    main()
