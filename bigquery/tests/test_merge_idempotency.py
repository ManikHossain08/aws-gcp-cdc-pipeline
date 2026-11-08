"""Executes the generated MERGE/DELETE SQL on DuckDB to prove idempotency and ordering."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

duckdb = pytest.importorskip("duckdb")

from bigquery import loader  # noqa: E402
from bigquery import sql_builder as sb  # noqa: E402
from bigquery.warehouse import DuckDBWarehouse  # noqa: E402
from cdc_common import metadata as m  # noqa: E402

RUN1, RUN2 = "cdc_20261105T090000Z", "cdc_20261105T100000Z"


def seq(n: int) -> str:
    return str(n).zfill(m.SEQUENCE_WIDTH)


@pytest.fixture
def wh(local_cfg):
    w = DuckDBWarehouse(local_cfg.duckdb_path)
    loader.deploy_ddl(local_cfg, w)
    yield w
    w.close()


def stage(cfg, wh, rows):
    """Replace orders staging with (order_id, status, op, seq, minute, run_id) tuples."""
    table = cfg.table("orders")
    wh.execute(f'DELETE FROM "{cfg.bq_staging_dataset}"."{table.staging_table}"')
    for oid, status, op, s, minute, run in rows:
        wh.conn.execute(
            f'INSERT INTO "{cfg.bq_staging_dataset}"."{table.staging_table}" '
            "(order_id, customer_id, order_date, status, total_amount, currency, metadata, "
            "created_at, updated_at, _cdc_operation, _cdc_timestamp, _source_sequence, "
            "_transaction_id, _source_file, _ingest_date, _pipeline_run_id) "
            "VALUES (?, 1, ?, ?, ?, 'USD', NULL, ?, NULL, ?, ?, ?, NULL, 'f', ?, ?)",
            [
                oid,
                date(2026, 11, 5),
                status,
                Decimal("1.00"),
                datetime(2026, 11, 5, tzinfo=timezone.utc),
                op,
                datetime(2026, 11, 5, 8, minute, tzinfo=timezone.utc),
                seq(s),
                date(2026, 11, 5),
                run,
            ],
        )


def apply(cfg, wh, run, strategy="soft"):
    t = cfg.table("orders")
    return (loader.merge_upserts(cfg, wh, t, run), loader.apply_deletes(cfg, wh, t, run, strategy))


def final(cfg, wh):
    rows, _ = wh.execute(
        f"SELECT order_id, status, _is_deleted, _source_sequence "
        f'FROM "{cfg.bq_final_dataset}"."orders" ORDER BY order_id'
    )
    return [
        (r["order_id"], r["status"], r["_is_deleted"], int(r["_source_sequence"])) for r in rows
    ]


def test_rerunning_same_batch_changes_nothing(local_cfg, wh):
    stage(local_cfg, wh, [(1, "NEW", "INSERT", 1, 1, RUN1), (2, "NEW", "INSERT", 2, 2, RUN1)])
    assert apply(local_cfg, wh, RUN1) == (2, 0)
    snapshot = final(local_cfg, wh)
    assert apply(local_cfg, wh, RUN1) == (0, 0)  # second run: no rows touched
    assert final(local_cfg, wh) == snapshot


def test_older_batch_arriving_late_is_ignored(local_cfg, wh):
    stage(local_cfg, wh, [(1, "SHIPPED", "UPDATE", 9, 9, RUN2)])
    apply(local_cfg, wh, RUN2)
    stage(local_cfg, wh, [(1, "PAID", "UPDATE", 5, 5, RUN1)])
    assert apply(local_cfg, wh, RUN1) == (0, 0)
    assert final(local_cfg, wh) == [(1, "SHIPPED", False, 9)]


def test_soft_delete_and_resurrection_ordering(local_cfg, wh):
    stage(local_cfg, wh, [(1, "NEW", "INSERT", 1, 1, RUN1)])
    apply(local_cfg, wh, RUN1)
    stage(local_cfg, wh, [(1, "NEW", "DELETE", 3, 3, RUN2)])
    assert apply(local_cfg, wh, RUN2) == (0, 1)
    assert final(local_cfg, wh) == [(1, "NEW", True, 3)]
    # a late, OLDER update must not resurrect the tombstone
    stage(local_cfg, wh, [(1, "PAID", "UPDATE", 2, 2, RUN1)])
    apply(local_cfg, wh, RUN1)
    assert final(local_cfg, wh) == [(1, "NEW", True, 3)]
    # a NEWER re-insert revives the key
    stage(local_cfg, wh, [(1, "REOPENED", "INSERT", 4, 4, RUN2)])
    apply(local_cfg, wh, RUN2)
    assert final(local_cfg, wh) == [(1, "REOPENED", False, 4)]


def test_hard_delete_removes_only_matching_keys(local_cfg, wh):
    stage(local_cfg, wh, [(1, "NEW", "INSERT", 1, 1, RUN1), (2, "NEW", "INSERT", 2, 2, RUN1)])
    apply(local_cfg, wh, RUN1)
    stage(local_cfg, wh, [(1, "NEW", "DELETE", 5, 5, RUN2), (99, "NEW", "DELETE", 6, 6, RUN2)])
    assert apply(local_cfg, wh, RUN2, "hard") == (0, 1)  # key 99 never existed: no-op
    assert final(local_cfg, wh) == [(2, "NEW", False, 2)]


def test_merge_only_reads_its_own_run(local_cfg, wh):
    stage(local_cfg, wh, [(1, "NEW", "INSERT", 1, 1, RUN1), (2, "OTHER", "INSERT", 2, 2, RUN2)])
    assert apply(local_cfg, wh, RUN1) == (1, 0)
    assert [r[0] for r in final(local_cfg, wh)] == [1]


def test_duplicate_rows_in_staging_apply_once(local_cfg, wh):
    stage(
        local_cfg,
        wh,
        [
            (1, "A", "UPDATE", 1, 1, RUN1),
            (1, "B", "UPDATE", 2, 2, RUN1),
            (1, "B", "UPDATE", 2, 2, RUN1),
        ],
    )
    apply(local_cfg, wh, RUN1)
    assert final(local_cfg, wh) == [(1, "B", False, 2)]


def test_audit_insert_accepts_nulls(local_cfg, wh):
    loader.insert_audit_row(
        local_cfg,
        wh,
        {
            "pipeline_run_id": RUN1,
            "table_name": "orders",
            "status": "SUCCEEDED",
            "started_at": "2026-11-05T09:00:00+00:00",
        },
    )
    rows, _ = wh.execute(f"SELECT * FROM {sb.audit_table_ref(local_cfg, sb.DUCKDB)}")
    assert rows[0]["status"] == "SUCCEEDED" and rows[0]["merged_row_count"] is None
