"""Ordering and de-duplication of validated CDC records."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from cdc_common import metadata as m
from cdc_common.config import TableConfig

_RN = "__rn"


def ordering_columns() -> list:
    """Total order of changes: sequence, then commit timestamp, then file (tie-break).

    The DMS change sequence is unique and monotonic per source, so it is the primary key of
    ordering; commit timestamps alone are ambiguous (many changes per millisecond / txn).
    The file name only makes exact duplicates deterministic.
    """
    return [
        F.col(m.SOURCE_SEQUENCE).desc(),
        F.col(m.CDC_TIMESTAMP).desc_nulls_last(),
        F.col(m.SOURCE_FILE).desc(),
    ]


def latest_per_key(df: DataFrame, table: TableConfig) -> DataFrame:
    """Keep only the latest change per primary key (one shuffle, partitioned by PK).

    De-duplication happens BEFORE splitting into upserts / deletes, so for every key the
    batch carries exactly one final operation (e.g. INSERT then DELETE -> DELETE).
    """
    window = Window.partitionBy(*table.primary_key).orderBy(*ordering_columns())
    return df.withColumn(_RN, F.row_number().over(window)).filter(F.col(_RN) == 1).drop(_RN)


def split_upserts_deletes(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Partition de-duplicated changes into (INSERT/UPDATE, DELETE)."""
    upserts = df.filter(F.col(m.CDC_OPERATION).isin(*m.UPSERT_OPS))
    deletes = df.filter(F.col(m.CDC_OPERATION) == m.OP_DELETE)
    return upserts, deletes
