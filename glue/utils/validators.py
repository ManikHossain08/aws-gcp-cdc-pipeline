"""Record-level validation and normalisation of raw DMS CDC records.

Every raw row gets an ``_errors`` array. Rows with no errors are typed and enriched with
pipeline metadata; rows with errors go to the rejected zone with the reasons and the
original record, so nothing is silently dropped.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from cdc_common import metadata as m
from cdc_common.config import CdcFields, TableConfig
from cdc_common.types import physical_types
from glue.utils.schemas import CORRUPT_RECORD, cdc_field_names

IS_FULL_LOAD = "__is_full_load"
ERRORS = "_errors"
_TYPED = "__typed__"

_OP_MAP = {
    "I": m.OP_INSERT,
    "INSERT": m.OP_INSERT,
    "U": m.OP_UPDATE,
    "UPDATE": m.OP_UPDATE,
    "D": m.OP_DELETE,
    "DELETE": m.OP_DELETE,
}


def normalize_operation(op: Column, is_full_load: Column) -> Column:
    """Map DMS ``I/U/D`` (or spelled-out values) to ``INSERT/UPDATE/DELETE``.

    Full-load rows without an operation are INSERTs. Anything else becomes NULL, which the
    validator reports as ``invalid_operation``.
    """
    cleaned = F.upper(F.trim(op))
    expr = F.when(cleaned.isNull() & is_full_load, F.lit(m.OP_INSERT))
    for raw, normalized in _OP_MAP.items():
        expr = expr.when(cleaned == raw, F.lit(normalized))
    return expr.otherwise(F.lit(None).cast("string"))


def try_cast(column: str, spark_ddl: str) -> Column:
    """ANSI-independent cast that yields NULL instead of failing."""
    return F.expr(f"try_cast(`{column}` AS {spark_ddl})")


def ingest_date_from_file(source_file: Column, fallback_ts: Column) -> Column:
    """Ingest date from ``.../YYYY/MM/DD/<file>`` or ``ingest_date=YYYY-MM-DD``; else ts date."""
    hive = F.regexp_extract(source_file, r"ingest_date=(\d{4}-\d{2}-\d{2})", 1)
    folders = F.regexp_extract(source_file, r"/(\d{4})/(\d{2})/(\d{2})/[^/]+$", 0)
    folder_date = F.regexp_replace(F.regexp_extract(folders, r"(\d{4}/\d{2}/\d{2})", 1), "/", "-")
    return F.coalesce(
        F.to_date(F.when(hive != "", hive)),
        F.to_date(F.when(folder_date != "", folder_date)),
        F.to_date(fallback_ts),
    )


def validate(df: DataFrame, table: TableConfig, fields: CdcFields) -> DataFrame:
    """Add ``_errors`` plus typed / normalised columns to raw string records.

    Rules:
      * malformed record (JSON parse failure)            -> ``malformed_record``
      * unknown / missing operation                      -> ``invalid_operation:<value>``
      * missing or non-castable primary key              -> ``missing_primary_key:<col>``
      * value not castable to the declared type          -> ``invalid_type:<col>:<type>``
      * NULL in a non-nullable column (non-delete rows)  -> ``missing_required:<col>``
      * missing / invalid commit timestamp               -> ``missing_cdc_timestamp``
      * CDC row without a numeric change sequence        -> ``invalid_sequence``
    """
    file_name = F.regexp_extract(F.col(m.SOURCE_FILE), r"([^/]+)$", 1)
    out = df.withColumn(IS_FULL_LOAD, file_name.startswith("LOAD"))
    out = out.withColumn(
        m.CDC_OPERATION, normalize_operation(F.col(fields.operation), F.col(IS_FULL_LOAD))
    )

    checks: list[Column] = [
        F.when(F.col(CORRUPT_RECORD).isNotNull(), F.lit("malformed_record")),
        F.when(
            F.col(CORRUPT_RECORD).isNull() & F.col(m.CDC_OPERATION).isNull(),
            F.concat(
                F.lit("invalid_operation:"), F.coalesce(F.col(fields.operation), F.lit("null"))
            ),
        ),
    ]

    for col in table.columns:
        ddl = physical_types(col.logical_type).spark
        typed_name = f"{_TYPED}{col.name}"
        out = out.withColumn(typed_name, try_cast(col.name, ddl))
        raw_present = F.col(col.name).isNotNull() & (F.trim(F.col(col.name)) != "")
        checks.append(
            F.when(
                raw_present & F.col(typed_name).isNull(),
                F.lit(f"invalid_type:{col.name}:{col.logical_type}"),
            )
        )
        if col.name in table.primary_key:
            checks.append(F.when(~raw_present, F.lit(f"missing_primary_key:{col.name}")))
        elif not col.nullable:
            checks.append(
                F.when(
                    ~raw_present & (F.col(m.CDC_OPERATION) != m.OP_DELETE),
                    F.lit(f"missing_required:{col.name}"),
                )
            )

    # Full-load rows may carry no commit timestamp; DMS's TimestampColumnName (load time) is
    # then the best ordering evidence. CDC rows must have a real commit timestamp.
    out = out.withColumn(
        m.CDC_TIMESTAMP,
        F.coalesce(
            try_cast(fields.cdc_timestamp, "timestamp"),
            F.when(F.col(IS_FULL_LOAD), try_cast(fields.ingest_timestamp, "timestamp")),
        ),
    )
    checks.append(F.when(F.col(m.CDC_TIMESTAMP).isNull(), F.lit("missing_cdc_timestamp")))

    seq = F.trim(F.col(fields.sequence))
    seq_missing = seq.isNull() | (seq == "")
    checks.append(F.when(~F.col(IS_FULL_LOAD) & seq_missing, F.lit("invalid_sequence")))
    checks.append(F.when(~seq_missing & ~seq.rlike(r"^\d{1,35}$"), F.lit("invalid_sequence")))
    out = out.withColumn(
        m.SOURCE_SEQUENCE,
        F.when(seq_missing, F.lit(m.FULL_LOAD_SEQUENCE)).otherwise(
            F.lpad(seq, m.SEQUENCE_WIDTH, "0")
        ),
    )

    return out.withColumn(ERRORS, F.filter(F.array(*checks), lambda e: e.isNotNull()))


def valid_records(df: DataFrame, table: TableConfig, fields: CdcFields, run_id: str) -> DataFrame:
    """Typed, normalised rows that passed validation (business columns + metadata)."""
    good = df.filter(F.size(F.col(ERRORS)) == 0)
    return good.select(
        *[F.col(f"{_TYPED}{c}").alias(c) for c in table.column_names],
        F.col(m.CDC_OPERATION),
        F.col(m.CDC_TIMESTAMP),
        F.col(m.SOURCE_SEQUENCE),
        F.col(fields.transaction_id).alias(m.TRANSACTION_ID),
        F.col(m.SOURCE_FILE),
        ingest_date_from_file(F.col(m.SOURCE_FILE), F.col(m.CDC_TIMESTAMP)).alias(m.INGEST_DATE),
        F.lit(run_id).alias(m.PIPELINE_RUN_ID),
    )


def rejected_records(
    df: DataFrame, table: TableConfig, fields: CdcFields, run_id: str
) -> DataFrame:
    """Rows that failed validation, with error metadata and the original record."""
    raw_cols = cdc_field_names(fields) + table.column_names
    bad = df.filter(F.size(F.col(ERRORS)) > 0)
    return bad.select(
        F.col(ERRORS),
        F.coalesce(F.col(CORRUPT_RECORD), F.to_json(F.struct(*raw_cols))).alias("_raw_record"),
        F.col(m.SOURCE_FILE),
        ingest_date_from_file(F.col(m.SOURCE_FILE), F.current_timestamp()).alias(m.INGEST_DATE),
        F.lit(run_id).alias(m.PIPELINE_RUN_ID),
        F.lit(table.name).alias("_table"),
        F.current_timestamp().alias("_rejected_at"),
    )
