"""Watermarks and raw-file discovery.

Decision: watermarks are JSON control files in the processed bucket
(``control/watermarks/<table>.json``). They sit next to the data they describe, survive
Airflow metadata-DB rebuilds, are versioned by the bucket, and work identically locally.

A watermark is advanced only after the run's MERGE and deletes succeeded, and never moves
backwards. Discovery re-lists ``overlap_minutes`` before the watermark; any re-read file
is harmless because the BigQuery MERGE ignores changes it already applied.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from cdc_common import paths
from cdc_common.config import PipelineConfig, TableConfig
from cdc_common.storage import ObjectInfo, ObjectStore, read_json, write_json

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True)
class Watermark:
    """Last raw-object modification time fully applied to BigQuery for a table."""

    table: str
    last_modified: datetime
    run_id: str | None = None
    updated_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        """JSON-serialisable form."""
        out = asdict(self)
        out["last_modified"] = self.last_modified.isoformat()
        out["updated_at"] = self.updated_at.isoformat() if self.updated_at else None
        return out


def read_watermark(cfg: PipelineConfig, store: ObjectStore, table: str) -> Watermark:
    """Current watermark, or the epoch when the table has never been processed."""
    key = paths.watermark_key(cfg, table)
    if not store.exists(key):
        return Watermark(table=table, last_modified=EPOCH)
    raw = read_json(store, key)
    return Watermark(
        table=table,
        last_modified=datetime.fromisoformat(raw["last_modified"]),
        run_id=raw.get("run_id"),
        updated_at=datetime.fromisoformat(raw["updated_at"]) if raw.get("updated_at") else None,
    )


def advance_watermark(
    cfg: PipelineConfig, store: ObjectStore, table: str, candidate: datetime, run_id: str
) -> Watermark:
    """Persist ``candidate`` if it is newer than the stored watermark (monotonic)."""
    current = read_watermark(cfg, store, table)
    if candidate <= current.last_modified:
        return current
    new = Watermark(table, candidate, run_id, datetime.now(timezone.utc))
    write_json(store, paths.watermark_key(cfg, table), new.to_dict())
    return new


def discover_files(
    cfg: PipelineConfig,
    raw_store: ObjectStore,
    table: TableConfig,
    watermark: Watermark,
    ingest_date: date | None = None,
) -> list[ObjectInfo]:
    """Raw CDC files to process for ``table``.

    * Incremental (default): files modified after ``watermark - overlap``.
    * Backfill (``ingest_date`` given): every file of that ingest date, watermark ignored.
    """
    suffix = ".parquet" if cfg.raw_format == "parquet" else ".jsonl"
    if ingest_date is not None:
        objects = raw_store.list(paths.raw_date_prefix(cfg, table, ingest_date))
        return [o for o in objects if o.key.endswith(suffix)]

    since = watermark.last_modified - timedelta(minutes=cfg.watermark_overlap_minutes)
    objects = raw_store.list(paths.raw_table_prefix(cfg, table))
    return [o for o in objects if o.key.endswith(suffix) and o.last_modified > since]
