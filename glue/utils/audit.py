"""Audit manifest construction and persistence for the Glue job."""

from __future__ import annotations

from datetime import datetime, timezone

from cdc_common import paths
from cdc_common.config import PipelineConfig
from cdc_common.manifest import GlueAuditManifest
from cdc_common.storage import ObjectStore, read_json, write_json


def utc_now_iso() -> str:
    """Current UTC time, ISO-8601."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def write_manifest(cfg: PipelineConfig, store: ObjectStore, manifest: GlueAuditManifest) -> str:
    """Persist ``manifest`` (written last for a table = commit marker). Returns its key."""
    key = paths.glue_manifest_key(cfg, manifest.table, manifest.pipeline_run_id)
    write_json(store, key, manifest.to_dict())
    return key


def read_manifest(
    cfg: PipelineConfig, store: ObjectStore, table: str, run_id: str
) -> GlueAuditManifest | None:
    """Load a table's manifest for ``run_id`` (None if the Glue job did not write one)."""
    key = paths.glue_manifest_key(cfg, table, run_id)
    if not store.exists(key):
        return None
    return GlueAuditManifest.from_dict(read_json(store, key))
