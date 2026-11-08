"""Pipeline metadata columns (names start with ``_``; reserved, never source columns)."""

from __future__ import annotations

CDC_OPERATION = "_cdc_operation"  # INSERT | UPDATE | DELETE
CDC_TIMESTAMP = "_cdc_timestamp"  # source commit timestamp
SOURCE_SEQUENCE = "_source_sequence"  # DMS AR_H_CHANGE_SEQ, zero-padded to 35 chars
TRANSACTION_ID = "_transaction_id"
SOURCE_FILE = "_source_file"
INGEST_DATE = "_ingest_date"
PIPELINE_RUN_ID = "_pipeline_run_id"
LOADED_AT = "_loaded_at"
IS_DELETED = "_is_deleted"

OP_INSERT, OP_UPDATE, OP_DELETE = "INSERT", "UPDATE", "DELETE"
UPSERT_OPS = (OP_INSERT, OP_UPDATE)

SEQUENCE_WIDTH = 35
FULL_LOAD_SEQUENCE = "0" * SEQUENCE_WIDTH  # full-load rows sort before any CDC event

# (name, logical type) of curated / staging records, after the business columns.
STAGE_METADATA: tuple[tuple[str, str], ...] = (
    (CDC_OPERATION, "string"),
    (CDC_TIMESTAMP, "timestamp"),
    (SOURCE_SEQUENCE, "string"),
    (TRANSACTION_ID, "string"),
    (SOURCE_FILE, "string"),
    (INGEST_DATE, "date"),
    (PIPELINE_RUN_ID, "string"),
)

# (name, logical type, description) of final tables, after the business columns.
FINAL_METADATA: tuple[tuple[str, str, str], ...] = (
    (CDC_OPERATION, "string", "Last applied CDC operation"),
    (CDC_TIMESTAMP, "timestamp", "Source commit timestamp of the last applied change"),
    (SOURCE_SEQUENCE, "string", "DMS change sequence of the last applied change (ordering guard)"),
    (PIPELINE_RUN_ID, "string", "Pipeline run that last changed the row (lineage)"),
    (LOADED_AT, "timestamp", "When the row was last written by the pipeline"),
    (IS_DELETED, "bool", "TRUE when deleted at source (soft-delete strategy)"),
)
