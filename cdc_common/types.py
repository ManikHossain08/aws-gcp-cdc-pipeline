"""Logical column types and their Spark / BigQuery / DuckDB equivalents."""

from __future__ import annotations

import re
from dataclasses import dataclass

_DECIMAL = re.compile(r"^decimal\((\d+),\s*(\d+)\)$")


class UnsupportedTypeError(ValueError):
    """Raised for a logical type the pipeline does not know how to map."""


@dataclass(frozen=True)
class PhysicalTypes:
    """One logical type rendered for each engine."""

    spark: str
    bigquery: str
    duckdb: str


_SIMPLE: dict[str, PhysicalTypes] = {
    "int64": PhysicalTypes("bigint", "INT64", "BIGINT"),
    "string": PhysicalTypes("string", "STRING", "VARCHAR"),
    "bool": PhysicalTypes("boolean", "BOOL", "BOOLEAN"),
    "float64": PhysicalTypes("double", "FLOAT64", "DOUBLE"),
    "date": PhysicalTypes("date", "DATE", "DATE"),
    "timestamp": PhysicalTypes("timestamp", "TIMESTAMP", "TIMESTAMPTZ"),
    # JSON kept as STRING end to end: parquet has no JSON type BigQuery loads natively.
    "json": PhysicalTypes("string", "STRING", "VARCHAR"),
}

# BigQuery NUMERIC = 29 integer digits + 9 fractional digits.
_BQ_NUMERIC_INT_DIGITS = 29
_BQ_NUMERIC_SCALE = 9


def physical_types(logical: str) -> PhysicalTypes:
    """Map a logical type from pipeline_config.yaml to engine-specific types."""
    normalized = logical.strip().lower().replace(" ", "")
    match = _DECIMAL.match(normalized)
    if match:
        precision, scale = int(match.group(1)), int(match.group(2))
        if scale > precision or precision > 38:
            raise UnsupportedTypeError(f"Unsupported decimal {logical!r} (max precision 38)")
        bq = (
            f"NUMERIC({precision},{scale})"
            if precision - scale <= _BQ_NUMERIC_INT_DIGITS and scale <= _BQ_NUMERIC_SCALE
            else f"BIGNUMERIC({precision},{scale})"
        )
        return PhysicalTypes(f"decimal({precision},{scale})", bq, f"DECIMAL({precision},{scale})")
    try:
        return _SIMPLE[normalized]
    except KeyError as exc:
        raise UnsupportedTypeError(f"Unknown logical type {logical!r}") from exc
