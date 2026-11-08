"""Spark read/write helpers for the raw and processed zones (S3 or local filesystem)."""

from __future__ import annotations

import math
import tempfile
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from cdc_common import metadata as m
from cdc_common.config import PipelineConfig, TableConfig
from cdc_common.manifest import OutputFile
from cdc_common.storage import ObjectStore, md5_file, spark_path
from glue.utils.schemas import CORRUPT_RECORD, raw_schema

DATA_SUFFIXES = (".parquet", ".json")


def read_raw(
    spark: SparkSession, cfg: PipelineConfig, table: TableConfig, keys: list[str]
) -> DataFrame:
    """Read the given raw DMS files as all-string records with an explicit schema.

    * JSONL: PERMISSIVE mode keeps malformed lines in ``_corrupt_record``.
    * Parquet (DMS in AWS): self-describing; projected onto the explicit column list
      (missing columns -> NULL) and cast to string so both formats share one validator.
    """
    paths = [spark_path(cfg.raw_uri, k) for k in keys]
    schema = raw_schema(table, cfg.cdc_fields)
    if cfg.raw_format == "jsonl":
        df = (
            spark.read.schema(schema)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", CORRUPT_RECORD)
            .json(paths)
        )
    else:
        parquet = spark.read.parquet(*paths)
        present = set(parquet.columns)
        df = parquet.select(
            *[
                (
                    F.col(f.name).cast("string")
                    if f.name in present
                    else F.lit(None).cast("string")
                ).alias(f.name)
                for f in schema.fields
                if f.name != CORRUPT_RECORD
            ]
        ).withColumn(CORRUPT_RECORD, F.lit(None).cast("string"))
    # Cache: Spark forbids queries referencing only _corrupt_record on an uncached JSON scan.
    return df.withColumn(m.SOURCE_FILE, F.input_file_name()).cache()


def write_parquet(
    df: DataFrame,
    cfg: PipelineConfig,
    key_prefix: str,
    row_count: int,
    rows_per_file: int,
    partition_by: list[str] | None = None,
) -> None:
    """Overwrite ``key_prefix`` with ``df`` as Snappy Parquet, sized by ``rows_per_file``.

    Overwrite is safe because every prefix is scoped to one pipeline run id.
    """
    num_files = max(1, math.ceil(row_count / rows_per_file))
    writer = df.repartition(num_files).write.mode("overwrite").option("compression", "snappy")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(spark_path(cfg.processed_uri, key_prefix))


def write_json(
    df: DataFrame, cfg: PipelineConfig, key_prefix: str, partition_by: list[str] | None = None
) -> None:
    """Overwrite ``key_prefix`` with ``df`` as JSON lines (rejected records stay readable)."""
    writer = df.coalesce(1).write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.json(spark_path(cfg.processed_uri, key_prefix))


def describe_outputs(store: ObjectStore, prefix: str) -> list[OutputFile]:
    """Data files under ``prefix`` with size and MD5 (computed by streaming when needed)."""
    files: list[OutputFile] = []
    for obj in store.list(prefix):
        name = obj.key.rsplit("/", 1)[-1]
        if not name.endswith(DATA_SUFFIXES) or name.startswith(("_", ".")):
            continue
        digest = store.md5(obj.key)
        if digest is None:
            with tempfile.TemporaryDirectory() as tmp:
                local = Path(tmp) / name
                store.download_file(obj.key, local)
                digest = md5_file(local)
        files.append(OutputFile(key=obj.key, size=obj.size, md5=digest))
    return files
