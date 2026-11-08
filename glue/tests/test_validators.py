"""CDC record parsing, operation normalisation and schema validation."""

from __future__ import annotations

import pytest

from cdc_common import metadata as m
from conftest import FIXTURES
from glue.utils import validators
from glue.utils.schemas import CORRUPT_RECORD, curated_columns, raw_schema, spark_type

pytestmark = pytest.mark.spark


def read_fixture(spark, cfg, name, as_full_load=False):
    from pyspark.sql import functions as F

    table = cfg.table("orders")
    path = str(FIXTURES / name)
    df = (
        spark.read.schema(raw_schema(table, cfg.cdc_fields))
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD)
        .json(path)
    )
    source = (
        "s3://raw/cdc/public/orders/LOAD00000001.jsonl"
        if as_full_load
        else "s3://raw/cdc/public/orders/2026/11/05/20261105-080000000.jsonl"
    )
    return validators.validate(
        df.withColumn(m.SOURCE_FILE, F.lit(source)).cache(), table, cfg.cdc_fields
    ), table


def test_valid_records_are_parsed_and_typed(spark, default_cfg):
    checked, table = read_fixture(spark, default_cfg, "insert.jsonl")
    valid = validators.valid_records(checked, table, default_cfg.cdc_fields, "cdc_20261105T090000Z")
    rows = {r["order_id"]: r for r in valid.collect()}
    assert set(rows) == {201, 202}
    assert str(rows[201]["total_amount"]) == "10.00"
    assert rows[201][m.CDC_OPERATION] == "INSERT"
    assert rows[201][m.SOURCE_SEQUENCE] == "20261105080001001000000000000000001"
    assert str(rows[201][m.INGEST_DATE]) == "2026-11-05"  # from YYYY/MM/DD folders
    assert valid.columns == curated_columns(table)


@pytest.mark.parametrize(
    ("raw", "full_load", "expected"),
    [
        ("I", False, "INSERT"),
        ("u", False, "UPDATE"),
        (" D ", False, "DELETE"),
        ("DELETE", False, "DELETE"),
        (None, True, "INSERT"),
        (None, False, None),
        ("X", False, None),
    ],
)
def test_operation_normalization(spark, raw, full_load, expected):
    from pyspark.sql import functions as F

    df = spark.createDataFrame([(raw, full_load)], "op string, fl boolean")
    out = df.select(validators.normalize_operation(F.col("op"), F.col("fl")).alias("n")).first()
    assert out["n"] == expected


def test_malformed_records_are_rejected_with_reasons(spark, default_cfg):
    checked, table = read_fixture(spark, default_cfg, "malformed.jsonl")
    errors = sorted(
        tuple(r[validators.ERRORS]) for r in checked.select(validators.ERRORS).collect()
    )
    flat = [e for errs in errors for e in errs]
    assert "malformed_record" in flat
    assert "invalid_operation:X" in flat
    assert "missing_primary_key:order_id" in flat
    assert "invalid_type:total_amount:decimal(12,2)" in flat
    assert "missing_cdc_timestamp" in flat
    assert all(errs for errs in errors)  # every malformed fixture line is rejected

    rejected = validators.rejected_records(
        checked, table, default_cfg.cdc_fields, "cdc_20261105T090000Z"
    )
    row = rejected.filter("array_contains(_errors, 'malformed_record')").first()
    assert row["_raw_record"].startswith('{"Op": "U"')  # original text preserved


def test_full_load_rows_get_zero_sequence(spark, default_cfg):
    checked, table = read_fixture(spark, default_cfg, "insert.jsonl", as_full_load=True)
    valid = validators.valid_records(checked, table, default_cfg.cdc_fields, "cdc_20261105T090000Z")
    # sequences present in the fixture are kept; zero-fill applies only when missing
    assert all(len(r[m.SOURCE_SEQUENCE]) == m.SEQUENCE_WIDTH for r in valid.collect())


def test_cdc_row_without_sequence_is_rejected(spark, default_cfg):
    from pyspark.sql import functions as F

    table = default_cfg.table("orders")
    f = default_cfg.cdc_fields
    rec = {
        f.operation: "U",
        f.cdc_timestamp: "2026-11-05T08:00:00+00:00",
        f.sequence: None,
        "order_id": "1",
        "customer_id": "1",
        "order_date": "2026-11-05",
        "status": "NEW",
        "created_at": "2026-11-05T08:00:00+00:00",
    }
    schema = raw_schema(table, f)
    row = tuple(rec.get(field.name) for field in schema.fields)
    df = spark.createDataFrame([row], schema).withColumn(
        m.SOURCE_FILE, F.lit("s3://raw/cdc/public/orders/2026/11/05/x.jsonl")
    )
    out = validators.validate(df, table, f).first()
    assert "invalid_sequence" in out[validators.ERRORS]


def test_spark_type_mapping_needs_no_jvm():
    assert spark_type("decimal(12,2)").simpleString() == "decimal(12,2)"
    assert spark_type("timestamp").simpleString() == "timestamp"
