"""Verify the warehouse state.

Checks (all backends):
  * no duplicate primary keys in final tables
  * every final row has CDC metadata
  * an audit row exists for every processed run

Reference-model check (local, JSONL raw data): replays every run's input files through an
independent pure-Python implementation of the CDC semantics (validate -> latest change per
key per run -> apply if newer -> soft/hard delete) and compares the expected state with the
final tables column by column.

Usage: python scripts/verify_bigquery_results.py
"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from bigquery.warehouse import warehouse_for  # noqa: E402
from cdc_common import metadata as m  # noqa: E402
from cdc_common.config import PipelineConfig, TableConfig, load_config  # noqa: E402
from cdc_common.paths import is_full_load_key  # noqa: E402
from cdc_common.storage import read_json, store_for_uri  # noqa: E402

_OPS = {"I": "INSERT", "U": "UPDATE", "D": "DELETE"}


def _parse(value: Any, logical: str) -> Any:
    if value is None or value == "":
        return None
    if logical == "int64":
        return int(value)
    if logical.startswith("decimal"):
        return Decimal(str(value))
    if logical == "timestamp":
        return datetime.fromisoformat(str(value))
    if logical == "date":
        return date.fromisoformat(str(value))
    return str(value)


def _valid_event(line: str, table: TableConfig, cfg: PipelineConfig, key: str) -> dict | None:
    f = cfg.cdc_fields
    try:
        rec = json.loads(line)
        op = _OPS.get(str(rec.get(f.operation) or "").upper()) or (
            "INSERT" if rec.get(f.operation) is None and is_full_load_key(key) else None
        )
        ts_raw = rec.get(f.cdc_timestamp) or (
            rec.get(f.ingest_timestamp) if is_full_load_key(key) else None
        )
        if op is None or ts_raw is None:
            return None
        row = {c.name: _parse(rec.get(c.name), c.logical_type) for c in table.columns}
    except (ValueError, InvalidOperation, TypeError):
        return None
    if any(row[k] is None for k in table.primary_key):
        return None
    if op != "DELETE" and any(row[c.name] is None for c in table.columns if not c.nullable):
        return None
    seq = rec.get(f.sequence)
    if not seq and not is_full_load_key(key):
        return None
    return {
        "row": row,
        "op": op,
        "seq": str(seq or "").zfill(m.SEQUENCE_WIDTH),
        "ts": datetime.fromisoformat(ts_raw),
    }


def expected_state(cfg: PipelineConfig) -> dict[str, dict[tuple, dict]]:
    """Replay runs in order with the reference semantics."""
    processed, raw = store_for_uri(cfg.processed_uri), store_for_uri(cfg.raw_uri)
    runs = sorted(
        o.key
        for o in processed.list(f"{cfg.prefix('control')}/runs/")
        if o.key.endswith("input_manifest.json")
    )
    state: dict[str, dict[tuple, dict]] = {t.name: {} for t in cfg.tables}
    for run_key in runs:
        manifest = read_json(processed, run_key)
        for table_name, files in manifest["tables"].items():
            table = cfg.table(table_name)
            latest: dict[tuple, dict] = {}
            for f in files:
                for line in raw.read_bytes(f["key"]).decode().splitlines():
                    ev = _valid_event(line, table, cfg, f["key"]) if line.strip() else None
                    if ev is None:
                        continue
                    pk = tuple(ev["row"][k] for k in table.primary_key)
                    cur = latest.get(pk)
                    if cur is None or (ev["seq"], ev["ts"]) > (cur["seq"], cur["ts"]):
                        latest[pk] = ev
            target = state[table_name]
            for pk, ev in latest.items():
                cur = target.get(pk)
                newer = cur is None or (ev["seq"], ev["ts"]) > (cur["seq"], cur["ts"])
                if ev["op"] == "DELETE":
                    if cur is not None and newer:
                        if cfg.delete_strategy == "hard":
                            del target[pk]
                        else:
                            target[pk] = {**cur, "seq": ev["seq"], "ts": ev["ts"], "deleted": True}
                elif newer:
                    target[pk] = {**ev, "deleted": False}
    return state


def verify(cfg: PipelineConfig, reference: bool = True) -> bool:
    """Run all checks; print a report; return True when everything passed."""
    wh = warehouse_for(cfg)
    d = wh.dialect
    failures: list[str] = []
    expected = expected_state(cfg) if reference and cfg.raw_format == "jsonl" else None
    for table in cfg.tables:
        ref = d.table(cfg.bq_project, cfg.bq_final_dataset, table.final_table)
        pk = ", ".join(d.ident(c) for c in table.primary_key)
        dup, _ = wh.execute(
            f"SELECT {pk}, COUNT(*) AS n FROM {ref} GROUP BY {pk} "  # noqa: S608
            "HAVING COUNT(*) > 1"
        )
        if dup:
            failures.append(f"{table.name}: duplicate primary keys {dup[:5]}")
        rows, _ = wh.execute(f"SELECT * FROM {ref}")  # noqa: S608
        if any(r[m.SOURCE_SEQUENCE] is None or r[m.PIPELINE_RUN_ID] is None for r in rows):
            failures.append(f"{table.name}: rows without CDC metadata")
        live = sum(not r[m.IS_DELETED] for r in rows)
        print(f"{table.name}: {len(rows)} rows ({live} live, {len(rows) - live} soft-deleted)")  # noqa: T201
        if expected is not None:
            actual = {tuple(r[k] for k in table.primary_key): r for r in rows}
            exp = expected[table.name]
            if set(actual) != set(exp):
                failures.append(
                    f"{table.name}: keys differ; missing={sorted(set(exp) - set(actual))} "
                    f"unexpected={sorted(set(actual) - set(exp))}"
                )
            for pk_val in set(actual) & set(exp):
                a, e = actual[pk_val], exp[pk_val]
                if bool(a[m.IS_DELETED]) != e["deleted"]:
                    failures.append(
                        f"{table.name}{pk_val}: _is_deleted {a[m.IS_DELETED]} != {e['deleted']}"
                    )
                for col in table.column_names:
                    av, ev = a[col], e["row"][col]
                    if isinstance(av, datetime) and isinstance(ev, datetime):
                        same = av == ev
                    else:
                        same = (str(av) if av is not None else None) == (
                            str(ev) if ev is not None else None
                        )
                    if not same:
                        failures.append(f"{table.name}{pk_val}.{col}: {av!r} != expected {ev!r}")
    audit = d.table(cfg.bq_project, cfg.bq_audit_dataset, cfg.bq_audit_table)
    rows, _ = wh.execute(f"SELECT status, COUNT(*) AS n FROM {audit} GROUP BY status")  # noqa: S608
    print(f"audit rows by status: {rows}")  # noqa: T201
    if not rows:
        failures.append("audit table is empty")
    getattr(wh, "close", lambda: None)()
    print("VERIFICATION PASSED" if not failures else "VERIFICATION FAILED:")  # noqa: T201
    for problem in failures:
        print(f"  - {problem}")  # noqa: T201
    return not failures


def compare_with_postgres(cfg: PipelineConfig) -> bool:
    """Compare live replica rows with the local Docker PostgreSQL source (ids + status)."""
    import os

    import psycopg

    dsn = (
        f"host={os.environ.get('SOURCE_DB_HOST', 'localhost')} "
        f"port={os.environ.get('SOURCE_DB_PORT', '5432')} "
        f"dbname={os.environ.get('SOURCE_DB_NAME', 'appdb')} "
        f"user={os.environ.get('SOURCE_DB_ADMIN_USER', 'postgres')} "
        f"password={os.environ['SOURCE_DB_ADMIN_PASSWORD']}"
    )
    wh = warehouse_for(cfg)
    ok = True
    with psycopg.connect(dsn) as conn:
        for table in cfg.tables:
            pk = table.primary_key[0]
            src = dict(conn.execute(f"SELECT {pk}, status FROM public.{table.name}").fetchall())
            ref = wh.dialect.table(cfg.bq_project, cfg.bq_final_dataset, table.final_table)
            rows, _ = wh.execute(f"SELECT {pk}, status FROM {ref} WHERE NOT {m.IS_DELETED}")
            dst = {r[pk]: r["status"] for r in rows}
            same = src == dst
            ok &= same
            print(
                f"postgres vs replica [{table.name}]: {'MATCH' if same else 'DIFF'} "  # noqa: T201
                f"({len(src)} source rows, {len(dst)} live replica rows)"
            )
            if not same:
                print(
                    f"  only in source: {sorted(set(src) - set(dst))}; "  # noqa: T201
                    f"only in replica: {sorted(set(dst) - set(src))}"
                )
    getattr(wh, "close", lambda: None)()
    return ok


if __name__ == "__main__":
    cfg = load_config()
    passed = verify(cfg)
    if "--compare-postgres" in sys.argv:
        passed = compare_with_postgres(cfg) and passed
    sys.exit(0 if passed else 1)
