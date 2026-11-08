"""Duplicate handling, ordering by sequence / timestamp, and DELETE handling."""

from __future__ import annotations

from datetime import datetime

import pytest

from cdc_common import metadata as m
from glue.utils.deduplication import latest_per_key, split_upserts_deletes

pytestmark = pytest.mark.spark

SCHEMA = (
    f"order_id bigint, status string, {m.CDC_OPERATION} string, "
    f"{m.CDC_TIMESTAMP} timestamp, {m.SOURCE_SEQUENCE} string, {m.SOURCE_FILE} string"
)


def seq(n: int) -> str:
    return str(n).zfill(m.SEQUENCE_WIDTH)


def ts(minute: int) -> datetime:
    return datetime(2026, 11, 5, 8, minute)


def run(spark, default_cfg, rows):
    df = spark.createDataFrame(rows, SCHEMA)
    return latest_per_key(df, default_cfg.table("orders"))


def test_exact_duplicates_collapse(spark, default_cfg):
    row = (1, "PAID", "UPDATE", ts(1), seq(5), "f1")
    out = run(spark, default_cfg, [row, row]).collect()
    assert len(out) == 1


def test_highest_sequence_wins_even_if_delivered_first(spark, default_cfg):
    rows = [
        (1, "DELIVERED", "UPDATE", ts(5), seq(7), "file_a"),  # newest, earlier file
        (1, "CANCELLED", "UPDATE", ts(4), seq(6), "file_b"),
    ]  # older, later file
    assert run(spark, default_cfg, rows).first()["status"] == "DELIVERED"


def test_sequence_beats_timestamp(spark, default_cfg):
    rows = [(1, "A", "UPDATE", ts(9), seq(1), "f"), (1, "B", "UPDATE", ts(1), seq(2), "f")]
    assert run(spark, default_cfg, rows).first()["status"] == "B"


def test_timestamp_breaks_sequence_ties(spark, default_cfg):
    rows = [
        (1, "A", "INSERT", ts(1), m.FULL_LOAD_SEQUENCE, "LOAD1"),
        (1, "B", "INSERT", ts(2), m.FULL_LOAD_SEQUENCE, "LOAD1"),
    ]
    assert run(spark, default_cfg, rows).first()["status"] == "B"


def test_cdc_event_wins_over_full_load_row(spark, default_cfg):
    rows = [
        (1, "LOADED", "INSERT", ts(30), m.FULL_LOAD_SEQUENCE, "LOAD1"),
        (1, "UPDATED", "UPDATE", ts(1), seq(1), "cdc"),
    ]
    assert run(spark, default_cfg, rows).first()["status"] == "UPDATED"


def test_insert_then_delete_nets_to_delete(spark, default_cfg):
    rows = [
        (1, "NEW", "INSERT", ts(1), seq(1), "f"),
        (1, "NEW", "DELETE", ts(2), seq(2), "f"),
        (2, "NEW", "INSERT", ts(3), seq(3), "f"),
    ]
    upserts, deletes = split_upserts_deletes(run(spark, default_cfg, rows))
    assert [r["order_id"] for r in deletes.collect()] == [1]
    assert [r["order_id"] for r in upserts.collect()] == [2]


def test_delete_then_reinsert_nets_to_insert(spark, default_cfg):
    rows = [(1, "NEW", "DELETE", ts(1), seq(1), "f"), (1, "NEW", "INSERT", ts(2), seq(2), "f")]
    upserts, deletes = split_upserts_deletes(run(spark, default_cfg, rows))
    assert deletes.count() == 0 and upserts.first()[m.CDC_OPERATION] == "INSERT"
