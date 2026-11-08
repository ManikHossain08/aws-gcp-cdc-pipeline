"""Audit manifests exchanged between Glue (producer), transfer and the BigQuery loader."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

MANIFEST_VERSION = 1


@dataclass(frozen=True)
class OutputFile:
    """One output file with integrity metadata."""

    key: str
    size: int
    md5: str


@dataclass
class GlueAuditManifest:
    """Per-table, per-run audit record written by the Glue job (commit marker)."""

    pipeline_run_id: str
    table: str
    status: str  # SUCCEEDED | FAILED
    input_file_count: int = 0
    input_row_count: int = 0
    valid_row_count: int = 0
    duplicate_row_count: int = 0
    upsert_row_count: int = 0
    delete_row_count: int = 0
    rejected_row_count: int = 0
    processing_started_at: str | None = None
    processing_finished_at: str | None = None
    input_files: list[str] = field(default_factory=list)
    outputs: dict[str, list[OutputFile]] = field(default_factory=dict)  # upserts / deletes
    rejected_prefix: str | None = None
    error: str | None = None
    manifest_version: int = MANIFEST_VERSION

    def to_dict(self) -> dict[str, Any]:
        """JSON-serialisable form."""
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> GlueAuditManifest:
        """Parse and validate a manifest."""
        if raw.get("manifest_version") != MANIFEST_VERSION:
            raise ValueError(f"Unsupported manifest_version {raw.get('manifest_version')!r}")
        outputs = {k: [OutputFile(**f) for f in v] for k, v in raw.get("outputs", {}).items()}
        return cls(**{**raw, "outputs": outputs})

    @property
    def rejected_ratio(self) -> float:
        """Share of input rows rejected by validation."""
        return self.rejected_row_count / self.input_row_count if self.input_row_count else 0.0

    def check_consistency(self) -> list[str]:
        """Arithmetic invariants every manifest must satisfy."""
        problems = []
        if self.input_row_count != self.valid_row_count + self.rejected_row_count:
            problems.append("input_row_count != valid_row_count + rejected_row_count")
        if self.valid_row_count != (
            self.upsert_row_count + self.delete_row_count + self.duplicate_row_count
        ):
            problems.append("valid_row_count != upserts + deletes + duplicates")
        if self.upsert_row_count and not self.outputs.get("upserts"):
            problems.append("upsert rows reported but no upsert files listed")
        if self.delete_row_count and not self.outputs.get("deletes"):
            problems.append("delete rows reported but no delete files listed")
        return problems
