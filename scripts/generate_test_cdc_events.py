"""Simulate AWS DMS output (full load + CDC) as JSONL in the raw "bucket".

Two modes:

* ``synthetic`` (default, no dependencies): deterministic events covering INSERT, UPDATE,
  DELETE, duplicate events for the same key, out-of-order delivery across files and
  malformed records. Batch 1 = full load + first CDC file; batch N>1 = more CDC.
* ``postgres``: runs real DML against the local Docker PostgreSQL (RETURNING *), uses the
  transaction id and WAL LSN as DMS-like metadata, and advances the replication slot.

Records mirror the DMS S3 target columns configured in dms/ (Op, _dms_commit_ts,
_dms_change_seq, _dms_transaction_id, _dms_source_table + business columns). Layout:
  <raw>/cdc/public/<table>/LOAD00000001.jsonl
  <raw>/cdc/public/<table>/YYYY/MM/DD/<YYYYMMDD-HHMMSSfff>.jsonl

Usage:
  python scripts/generate_test_cdc_events.py --batch 1
  python scripts/generate_test_cdc_events.py --batch 2
  python scripts/generate_test_cdc_events.py --mode postgres --batch 1
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cdc_common.config import PipelineConfig, load_config  # noqa: E402
from cdc_common.logging_utils import get_logger  # noqa: E402
from cdc_common.paths import raw_table_prefix  # noqa: E402
from cdc_common.storage import ObjectStore, store_for_uri  # noqa: E402

log = get_logger("generate_test_cdc_events")

SEED_CUSTOMERS = [
    (1, "ada.lovelace@example.com", "Ada Lovelace", "GB", "active", "2026-10-01T09:00:00+00:00"),
    (2, "alan.turing@example.com", "Alan Turing", "GB", "active", "2026-10-02T10:30:00+00:00"),
    (3, "grace.hopper@example.com", "Grace Hopper", "US", "active", "2026-10-03T11:15:00+00:00"),
    (
        4,
        "katherine.j@example.com",
        "Katherine Johnson",
        "US",
        "active",
        "2026-10-04T08:45:00+00:00",
    ),
    (5, "edsger.d@example.com", "Edsger Dijkstra", "NL", "suspended", "2026-10-05T14:00:00+00:00"),
]
SEED_ORDERS = [
    (
        101,
        1,
        "2026-11-01",
        "PAID",
        "120.50",
        "GBP",
        '{"channel": "web"}',
        "2026-11-01T09:10:00+00:00",
    ),
    (
        102,
        1,
        "2026-11-02",
        "SHIPPED",
        "35.00",
        "GBP",
        '{"channel": "mobile"}',
        "2026-11-02T12:00:00+00:00",
    ),
    (
        103,
        2,
        "2026-11-02",
        "NEW",
        "999.99",
        "GBP",
        '{"channel": "web"}',
        "2026-11-02T15:30:00+00:00",
    ),
    (
        104,
        3,
        "2026-11-03",
        "DELIVERED",
        "42.00",
        "USD",
        '{"channel": "store"}',
        "2026-11-03T16:45:00+00:00",
    ),
    (105, 4, "2026-11-04", "NEW", "15.25", "USD", None, "2026-11-04T18:20:00+00:00"),
]


def customer(cid, email, name, cc, status, created, updated=None) -> dict[str, Any]:
    """Customer business columns."""
    return {
        "customer_id": cid,
        "email": email,
        "full_name": name,
        "country_code": cc,
        "status": status,
        "created_at": created,
        "updated_at": updated,
    }


def order(oid, cid, day, status, amount, cur, meta, created, updated=None) -> dict[str, Any]:
    """Order business columns."""
    return {
        "order_id": oid,
        "customer_id": cid,
        "order_date": day,
        "status": status,
        "total_amount": amount,
        "currency": cur,
        "metadata": meta,
        "created_at": created,
        "updated_at": updated,
    }


class EventWriter:
    """Builds DMS-shaped records with monotonic change sequences."""

    def __init__(self, cfg: PipelineConfig, batch: int, start: datetime) -> None:
        self.f = cfg.cdc_fields
        self.batch = batch
        self.clock = start
        self.counter = 0
        self.txn = 9000 + batch * 100

    def seq(self, at: datetime) -> str:
        """35-digit sequence: timestamp(14) + batch(3) + counter(18)."""
        self.counter += 1
        return f"{at:%Y%m%d%H%M%S}{self.batch:03d}{self.counter:018d}"

    def event(
        self,
        table: str,
        op: str,
        row: dict[str, Any],
        *,
        full_load: bool = False,
        txn: int | None = None,
    ) -> dict[str, Any]:
        """One CDC record; full-load rows have no change sequence (like DMS)."""
        self.clock += timedelta(seconds=1)
        return {
            self.f.operation: op,
            self.f.cdc_timestamp: self.clock.isoformat(),
            self.f.sequence: None if full_load else self.seq(self.clock),
            self.f.transaction_id: None if full_load else str(txn or self.txn),
            self.f.source_table: table,
            **row,
        }


def write_jsonl(store: ObjectStore, key: str, records: list[Any]) -> None:
    """Write records (dicts, or raw strings for malformed lines) as JSONL."""
    lines = [r if isinstance(r, str) else json.dumps(r) for r in records]
    store.write_bytes(key, ("\n".join(lines) + "\n").encode(), "application/x-ndjson")
    log.info("raw_file_written", extra={"key": key, "records": len(records)})


def cdc_key(cfg: PipelineConfig, table: str, at: datetime) -> str:
    """DMS CDC file key with date folders."""
    tbl = cfg.table(table)
    return f"{raw_table_prefix(cfg, tbl)}{at:%Y/%m/%d}/{at:%Y%m%d-%H%M%S%f}"[:-3] + ".jsonl"


def synthetic(cfg: PipelineConfig, store: ObjectStore, batch: int) -> None:
    """Deterministic scenario covering every CDC edge case."""
    base = datetime(2026, 11, 5, 8, 0, tzinfo=timezone.utc) + timedelta(hours=batch - 1)
    w = EventWriter(cfg, batch, base)

    if batch == 1:
        for tbl, rows in (
            ("customers", [customer(*c) for c in SEED_CUSTOMERS]),
            ("orders", [order(*o) for o in SEED_ORDERS]),
        ):
            records = [w.event(tbl, "I", r, full_load=True) for r in rows]
            write_jsonl(
                store, f"{raw_table_prefix(cfg, cfg.table(tbl))}LOAD00000001.jsonl", records
            )

        c6 = customer(
            6,
            "barbara.liskov@example.com",
            "Barbara Liskov",
            "US",
            "active",
            "2026-11-05T08:00:30+00:00",
        )
        cust = [
            w.event("customers", "I", c6),
            w.event(
                "customers",
                "U",
                {
                    **customer(*SEED_CUSTOMERS[1]),
                    "status": "suspended",
                    "updated_at": "2026-11-05T08:01:00+00:00",
                },
            ),
            w.event("customers", "D", customer(*SEED_CUSTOMERS[4])),
        ]
        o106 = order(
            106,
            6,
            "2026-11-05",
            "NEW",
            "250.00",
            "USD",
            '{"channel": "web"}',
            "2026-11-05T08:00:40+00:00",
        )
        # Two files for orders: the SECOND file holds OLDER events (out-of-order delivery).
        early, late = [], []
        early.append(w.event("orders", "I", o106))
        late_upd = w.event(
            "orders",
            "U",
            {**order(*SEED_ORDERS[2]), "status": "PAID", "updated_at": "2026-11-05T08:02:00+00:00"},
        )
        newest = w.event(
            "orders",
            "U",
            {
                **order(*SEED_ORDERS[2]),
                "status": "SHIPPED",
                "updated_at": "2026-11-05T08:03:00+00:00",
            },
        )
        early.append(newest)
        late.append(late_upd)  # older than `newest` but delivered later
        # Duplicate delivery of the same event (at-least-once semantics).
        dup = w.event(
            "orders",
            "U",
            {
                **order(*SEED_ORDERS[0]),
                "status": "SHIPPED",
                "updated_at": "2026-11-05T08:04:00+00:00",
            },
        )
        early.extend([dup, dict(dup)])
        early.append(w.event("orders", "D", order(*SEED_ORDERS[4])))
        # Insert then delete of the same key inside one batch -> net delete (no row).
        o107 = order(107, 3, "2026-11-05", "NEW", "5.00", "USD", None, "2026-11-05T08:05:00+00:00")
        early.extend([w.event("orders", "I", o107), w.event("orders", "D", o107)])
        # Malformed records -> rejected zone.
        late.extend(
            [
                '{"Op": "U", "order_id": 101, "status": "PAID"',  # broken JSON
                json.dumps(w.event("orders", "X", order(*SEED_ORDERS[1]))),  # bad op
                json.dumps(w.event("orders", "I", {**o106, "order_id": None})),  # null PK
                json.dumps(
                    w.event(
                        "orders", "I", {**o106, "order_id": 108, "total_amount": "not-a-number"}
                    )
                ),  # bad type
            ]
        )
        write_jsonl(store, cdc_key(cfg, "customers", base + timedelta(minutes=10)), cust)
        write_jsonl(store, cdc_key(cfg, "orders", base + timedelta(minutes=10)), early)
        write_jsonl(store, cdc_key(cfg, "orders", base + timedelta(minutes=11)), late)
    else:
        # Later batch: re-insert a previously deleted key, update, delete, late older event.
        cust = [
            w.event(
                "customers", "I", customer(*SEED_CUSTOMERS[4][:4], "active", SEED_CUSTOMERS[4][5])
            )
        ]
        orders_ = [
            w.event(
                "orders",
                "U",
                {**order(*SEED_ORDERS[3]), "status": "CANCELLED", "updated_at": base.isoformat()},
            ),
            w.event("orders", "D", order(*SEED_ORDERS[1])),
            w.event(
                "orders",
                "I",
                order(
                    100 + batch * 10,
                    1,
                    "2026-11-05",
                    "NEW",
                    "12.00",
                    "EUR",
                    '{"channel": "api"}',
                    base.isoformat(),
                ),
            ),
        ]
        write_jsonl(store, cdc_key(cfg, "customers", base + timedelta(minutes=10)), cust)
        write_jsonl(store, cdc_key(cfg, "orders", base + timedelta(minutes=10)), orders_)


def from_postgres(cfg: PipelineConfig, store: ObjectStore, batch: int) -> None:
    """Apply DML to the local PostgreSQL container and emit DMS-like records."""
    import psycopg  # local-dev dependency
    from psycopg.rows import dict_row

    dsn = (
        f"host={os.environ.get('SOURCE_DB_HOST', 'localhost')} "
        f"port={os.environ.get('SOURCE_DB_PORT', '5432')} "
        f"dbname={os.environ.get('SOURCE_DB_NAME', 'appdb')} "
        f"user={os.environ.get('SOURCE_DB_ADMIN_USER', 'postgres')} "
        f"password={os.environ['SOURCE_DB_ADMIN_PASSWORD']}"
    )
    f = cfg.cdc_fields

    def to_json_value(v: Any) -> Any:
        return (
            v.isoformat()
            if hasattr(v, "isoformat")
            else (str(v) if v is not None and not isinstance(v, (int, float, str, bool)) else v)
        )

    def record(
        table: str, op: str, row: dict[str, Any], lsn: int | None, txn: int | None, ts: datetime
    ) -> dict[str, Any]:
        tbl = cfg.table(table)
        clean = {c: to_json_value(row.get(c)) for c in tbl.column_names}
        if isinstance(row.get("metadata"), dict):
            clean["metadata"] = json.dumps(row["metadata"])
        return {
            f.operation: op,
            f.cdc_timestamp: ts.isoformat(),
            f.sequence: None if lsn is None else f"{lsn:035d}",
            f.transaction_id: None if txn is None else str(txn),
            f.source_table: table,
            **clean,
        }

    with psycopg.connect(dsn, row_factory=dict_row, autocommit=False) as conn:
        now = datetime.now(timezone.utc)
        if batch == 1:  # full load snapshot
            for table in ("customers", "orders"):
                rows = conn.execute(f"SELECT * FROM public.{table} ORDER BY 1").fetchall()  # noqa: S608
                write_jsonl(
                    store,
                    f"{raw_table_prefix(cfg, cfg.table(table))}LOAD00000001.jsonl",
                    [record(table, "I", r, None, None, now) for r in rows],
                )
        # DELETE first: removes the order generated by the PREVIOUS batch (cross-batch delete).
        statements = [
            (
                "orders",
                "D",
                "DELETE FROM public.orders WHERE order_id = (SELECT min(order_id) "
                "FROM public.orders WHERE total_amount = 19.99) RETURNING *",
                (),
            ),
            (
                "customers",
                "I",
                "INSERT INTO public.customers (email, full_name, country_code) "
                "VALUES (%s, 'Generated User', 'DE') RETURNING *",
                (f"user{batch}_{int(now.timestamp())}@example.com",),
            ),
            (
                "orders",
                "U",
                "UPDATE public.orders SET status = 'PAID' "
                "WHERE order_id = (SELECT min(order_id) FROM public.orders "
                "WHERE status = 'NEW') RETURNING *",
                (),
            ),
            (
                "orders",
                "I",
                "INSERT INTO public.orders (customer_id, status, total_amount) "
                "VALUES (1, 'NEW', 19.99) RETURNING *",
                (),
            ),
        ]
        out: dict[str, list[dict[str, Any]]] = {"customers": [], "orders": []}
        for table, op, sql, params in statements:
            with conn.transaction():
                rows = conn.execute(sql, params).fetchall()
                meta = conn.execute(
                    "SELECT txid_current() AS txn, "
                    "pg_current_wal_lsn() - '0/0'::pg_lsn AS lsn, "
                    "clock_timestamp() AS ts"
                ).fetchone()
            assert meta is not None
            out[table].extend(
                record(table, op, r, int(meta["lsn"]), int(meta["txn"]), meta["ts"]) for r in rows
            )
        # We are the slot's consumer locally: advance it so it does not retain WAL.
        conn.execute("SELECT pg_replication_slot_advance('dms_cdc_slot', pg_current_wal_lsn())")
        conn.commit()
    for table, records in out.items():
        if records:
            write_jsonl(store, cdc_key(cfg, table, now), records)


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--mode", choices=["synthetic", "postgres"], default="synthetic")
    parser.add_argument("--batch", type=int, default=1)
    args = parser.parse_args()
    cfg = load_config()
    if cfg.raw_format != "jsonl":
        raise SystemExit("Simulator writes JSONL; set RAW_FORMAT=jsonl")
    store = store_for_uri(cfg.raw_uri)
    (synthetic if args.mode == "synthetic" else from_postgres)(cfg, store, args.batch)


if __name__ == "__main__":
    main()
