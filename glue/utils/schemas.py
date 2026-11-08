"""Explicit Spark schemas for raw CDC records and curated output.

Raw records are read with an explicit schema in which every field is a STRING. Typing
happens afterwards in ``validators`` with ``try_cast`` so that a single bad value becomes a
*rejected record with an error message* instead of a silently-null column (inference) or a
failed job (strict typed read).
"""

from __future__ import annotations

import re

from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from cdc_common import metadata as m
from cdc_common.config import CdcFields, TableConfig
from cdc_common.types import physical_types

CORRUPT_RECORD = "_corrupt_record"


def cdc_field_names(fields: CdcFields) -> list[str]:
    """DMS metadata column names in a stable order."""
    return [
        fields.operation,
        fields.cdc_timestamp,
        fields.ingest_timestamp,
        fields.sequence,
        fields.transaction_id,
        fields.source_table,
    ]


def raw_schema(table: TableConfig, fields: CdcFields, *, with_corrupt: bool = True) -> StructType:
    """All-string schema for raw DMS records of ``table`` (explicit, never inferred)."""
    names = cdc_field_names(fields) + table.column_names
    struct = [StructField(n, StringType(), True) for n in names]
    if with_corrupt:
        struct.append(StructField(CORRUPT_RECORD, StringType(), True))
    return StructType(struct)


_SPARK_TYPES: dict[str, DataType] = {
    "bigint": LongType(),
    "string": StringType(),
    "boolean": BooleanType(),
    "double": DoubleType(),
    "date": DateType(),
    "timestamp": TimestampType(),
}
_DECIMAL = re.compile(r"^decimal\((\d+),(\d+)\)$")


def spark_type(logical: str) -> DataType:
    """Spark ``DataType`` for a logical type (no SparkContext required)."""
    ddl = physical_types(logical).spark
    match = _DECIMAL.match(ddl)
    if match:
        return DecimalType(int(match.group(1)), int(match.group(2)))
    return _SPARK_TYPES[ddl]


def curated_schema(table: TableConfig) -> StructType:
    """Typed schema of validated / deduplicated output (= BigQuery staging table)."""
    business = [
        StructField(
            c.name, spark_type(c.logical_type), c.nullable or c.name not in table.primary_key
        )
        for c in table.columns
    ]
    meta = [StructField(name, spark_type(t), True) for name, t in m.STAGE_METADATA]
    return StructType(business + meta)


def curated_columns(table: TableConfig) -> list[str]:
    """Ordered output column names: business columns then metadata."""
    return table.column_names + [name for name, _ in m.STAGE_METADATA]
