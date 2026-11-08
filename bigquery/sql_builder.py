"""SQL generation for BigQuery (production) and DuckDB (local stand-in).

The same statements run on both engines; only identifier quoting, parameter markers,
types and a few DDL clauses differ (``Dialect``). That lets the local pipeline and the
idempotency tests execute the *real* MERGE logic.

Correctness rules:
1. Source rows are de-duplicated again (QUALIFY) — the loader never trusts upstream.
2. A change is applied only if it is newer than the row in the target:
   ``S.seq > T.seq OR (S.seq = T.seq AND S.ts > T.ts)``. Re-running a batch, or receiving
   an older batch late, is a no-op => idempotent and order-safe.
3. Deletes only touch matching primary keys. Soft: tombstone (``_is_deleted = TRUE``, keeps
   the sequence so a late older update cannot resurrect the row). Hard: ``DELETE``.
"""

from __future__ import annotations

from dataclasses import dataclass

from cdc_common import metadata as m
from cdc_common.config import PipelineConfig, TableConfig
from cdc_common.types import physical_types

RUN_ID_PARAM = "run_id"


@dataclass(frozen=True)
class Dialect:
    """Engine-specific rendering."""

    name: str  # bigquery | duckdb

    def ident(self, name: str) -> str:
        """Quote a column identifier."""
        if any(ch in name for ch in '`"\\;'):
            raise ValueError(f"Illegal identifier {name!r}")
        return f"`{name}`" if self.name == "bigquery" else f'"{name}"'

    def table(self, project: str, dataset: str, table: str) -> str:
        """Fully-qualified table reference."""
        for part in (project, dataset, table):
            self.ident(part)
        if self.name == "bigquery":
            return f"`{project}.{dataset}.{table}`"
        return f'"{dataset}"."{table}"'

    def param(self, name: str) -> str:
        """Named query parameter marker."""
        return f"@{name}" if self.name == "bigquery" else f"${name}"

    @property
    def now(self) -> str:
        """Current timestamp expression."""
        return "CURRENT_TIMESTAMP()" if self.name == "bigquery" else "current_timestamp"

    def type(self, logical: str) -> str:
        """Column type for a logical type."""
        types = physical_types(logical)
        return types.bigquery if self.name == "bigquery" else types.duckdb


BIGQUERY = Dialect("bigquery")
DUCKDB = Dialect("duckdb")


def dialect_for(cfg: PipelineConfig) -> Dialect:
    """Dialect matching the configured warehouse backend."""
    return BIGQUERY if cfg.bq_backend == "bigquery" else DUCKDB


def _final(cfg: PipelineConfig, t: TableConfig, d: Dialect) -> str:
    return d.table(cfg.bq_project, cfg.bq_final_dataset, t.final_table)


def _stage(cfg: PipelineConfig, t: TableConfig, d: Dialect) -> str:
    return d.table(cfg.bq_project, cfg.bq_staging_dataset, t.staging_table)


def audit_table_ref(cfg: PipelineConfig, d: Dialect) -> str:
    """Reference of the audit table."""
    return d.table(cfg.bq_project, cfg.bq_audit_dataset, cfg.bq_audit_table)


def _newer(d: Dialect) -> str:
    seq, ts = d.ident(m.SOURCE_SEQUENCE), d.ident(m.CDC_TIMESTAMP)
    return f"(S.{seq} > COALESCE(T.{seq}, '') OR (S.{seq} = T.{seq} AND S.{ts} > T.{ts}))"


def _latest_source(
    cfg: PipelineConfig, t: TableConfig, d: Dialect, cols: list[str], ops: tuple[str, ...]
) -> str:
    op_list = ", ".join(f"'{op}'" for op in ops)
    pk = ", ".join(d.ident(c) for c in t.primary_key)
    return (
        f"SELECT {', '.join(d.ident(c) for c in cols)}\n"
        f"  FROM {_stage(cfg, t, d)}\n"
        f"  WHERE {d.ident(m.PIPELINE_RUN_ID)} = {d.param(RUN_ID_PARAM)}\n"
        f"    AND {d.ident(m.CDC_OPERATION)} IN ({op_list})\n"
        f"  QUALIFY ROW_NUMBER() OVER (\n"
        f"    PARTITION BY {pk}\n"
        f"    ORDER BY {d.ident(m.SOURCE_SEQUENCE)} DESC, {d.ident(m.CDC_TIMESTAMP)} DESC\n"
        f"  ) = 1"
    )


def _on(t: TableConfig, d: Dialect) -> str:
    return " AND ".join(f"T.{d.ident(c)} = S.{d.ident(c)}" for c in t.primary_key)


def build_merge_upserts(cfg: PipelineConfig, t: TableConfig, d: Dialect = BIGQUERY) -> str:
    """MERGE the run's INSERT/UPDATE changes from staging into the final table."""
    meta = [m.CDC_OPERATION, m.CDC_TIMESTAMP, m.SOURCE_SEQUENCE, m.PIPELINE_RUN_ID]
    cols = t.column_names + meta
    non_key = [c for c in t.column_names if c not in t.primary_key]
    sets = [f"{d.ident(c)} = S.{d.ident(c)}" for c in non_key + meta] + [
        f"{d.ident(m.LOADED_AT)} = {d.now}",
        f"{d.ident(m.IS_DELETED)} = FALSE",  # a newer insert/update revives a soft-deleted key
    ]
    insert_cols = cols + [m.LOADED_AT, m.IS_DELETED]
    insert_vals = [f"S.{d.ident(c)}" for c in cols] + [d.now, "FALSE"]
    return (
        f"MERGE INTO {_final(cfg, t, d)} AS T\n"
        f"USING (\n  {_latest_source(cfg, t, d, cols, m.UPSERT_OPS)}\n) AS S\n"
        f"ON {_on(t, d)}\n"
        f"WHEN MATCHED AND {_newer(d)} THEN\n"
        f"  UPDATE SET\n    " + ",\n    ".join(sets) + "\n"
        f"WHEN NOT MATCHED THEN\n"
        f"  INSERT ({', '.join(d.ident(c) for c in insert_cols)})\n"
        f"  VALUES ({', '.join(insert_vals)})"
    )


def build_apply_deletes(
    cfg: PipelineConfig, t: TableConfig, strategy: str, d: Dialect = BIGQUERY
) -> str:
    """Apply the run's DELETE changes to matching primary keys (soft or hard)."""
    if strategy not in {"soft", "hard"}:
        raise ValueError(f"Unknown delete strategy {strategy!r}")
    cols = list(t.primary_key) + [m.CDC_TIMESTAMP, m.SOURCE_SEQUENCE, m.PIPELINE_RUN_ID]
    if strategy == "hard":
        action = "DELETE"
    else:
        action = "UPDATE SET\n    " + ",\n    ".join(
            [
                f"{d.ident(m.IS_DELETED)} = TRUE",
                f"{d.ident(m.CDC_OPERATION)} = '{m.OP_DELETE}'",
                f"{d.ident(m.CDC_TIMESTAMP)} = S.{d.ident(m.CDC_TIMESTAMP)}",
                f"{d.ident(m.SOURCE_SEQUENCE)} = S.{d.ident(m.SOURCE_SEQUENCE)}",
                f"{d.ident(m.PIPELINE_RUN_ID)} = S.{d.ident(m.PIPELINE_RUN_ID)}",
                f"{d.ident(m.LOADED_AT)} = {d.now}",
            ]
        )
    return (
        f"MERGE INTO {_final(cfg, t, d)} AS T\n"
        f"USING (\n  {_latest_source(cfg, t, d, cols, (m.OP_DELETE,))}\n) AS S\n"
        f"ON {_on(t, d)}\n"
        f"WHEN MATCHED AND {_newer(d)} THEN\n  {action}"
    )


# --------------------------------------------------------------------------- DDL
def build_create_datasets(cfg: PipelineConfig, d: Dialect = BIGQUERY) -> list[str]:
    """CREATE SCHEMA statements for staging, final and audit datasets."""
    stmts = []
    for ds in (cfg.bq_staging_dataset, cfg.bq_final_dataset, cfg.bq_audit_dataset):
        if d.name == "bigquery":
            stmts.append(
                f"CREATE SCHEMA IF NOT EXISTS `{cfg.bq_project}.{ds}`\n"
                f"  OPTIONS (location = '{cfg.bq_location}')"
            )
        else:
            stmts.append(f'CREATE SCHEMA IF NOT EXISTS "{ds}"')
    return stmts


def _column_defs(
    t: TableConfig,
    d: Dialect,
    meta: list[tuple[str, str]],
    descriptions: dict[str, str] | None = None,
    *,
    not_null_pk_only: bool = False,
) -> list[str]:
    defs = []
    for c in t.columns:
        line = f"{d.ident(c.name)} {d.type(c.logical_type)}"
        required = c.name in t.primary_key if not_null_pk_only else not c.nullable
        if required and d.name == "bigquery":
            line += " NOT NULL"
        if c.pii and d.name == "bigquery":
            line += " OPTIONS (description = 'PII')"
        defs.append(line)
    for name, logical in meta:
        line = f"{d.ident(name)} {d.type(logical)}"
        if descriptions and d.name == "bigquery" and name in descriptions:
            line += f" OPTIONS (description = '{descriptions[name]}')"
        defs.append(line)
    return defs


def build_create_final_table(cfg: PipelineConfig, t: TableConfig, d: Dialect = BIGQUERY) -> str:
    """Final (analytics) table: business columns + CDC metadata, partitioned and clustered."""
    meta = [(n, lt) for n, lt, _ in m.FINAL_METADATA]
    desc = {n: text for n, _, text in m.FINAL_METADATA}
    defs = _column_defs(t, d, meta, desc)
    if d.name == "bigquery":
        defs.append(f"PRIMARY KEY ({', '.join(d.ident(c) for c in t.primary_key)}) NOT ENFORCED")
    sql = f"CREATE TABLE IF NOT EXISTS {_final(cfg, t, d)} (\n  " + ",\n  ".join(defs) + "\n)"
    if d.name == "bigquery":
        if t.partition_field:
            ptype = t.column(t.partition_field).logical_type
            field = d.ident(t.partition_field)
            sql += f"\nPARTITION BY {field if ptype == 'date' else f'DATE({field})'}"
        if t.cluster_by:
            sql += f"\nCLUSTER BY {', '.join(d.ident(c) for c in t.cluster_by)}"
        sql += (
            f"\nOPTIONS (description = 'CDC replica of {t.source_schema}.{t.name}; "
            f"grain: one row per {', '.join(t.primary_key)}')"
        )
    return sql


def build_create_staging_table(cfg: PipelineConfig, t: TableConfig, d: Dialect = BIGQUERY) -> str:
    """Staging table: exactly the Glue deduplicated output schema.

    Only the primary key is NOT NULL: delete images may lack other required columns
    (REPLICA IDENTITY DEFAULT), and they must still load.
    """
    defs = _column_defs(t, d, list(m.STAGE_METADATA), not_null_pk_only=True)
    sql = f"CREATE TABLE IF NOT EXISTS {_stage(cfg, t, d)} (\n  " + ",\n  ".join(defs) + "\n)"
    if d.name == "bigquery":
        sql += f"\nOPTIONS (description = 'Per-run CDC staging for {t.name}; truncated each load')"
    return sql


AUDIT_COLUMNS: tuple[tuple[str, str], ...] = (
    ("pipeline_run_id", "string"),
    ("dag_run_id", "string"),
    ("table_name", "string"),
    ("status", "string"),
    ("input_file_count", "int64"),
    ("input_row_count", "int64"),
    ("valid_row_count", "int64"),
    ("duplicate_row_count", "int64"),
    ("upsert_row_count", "int64"),
    ("delete_row_count", "int64"),
    ("rejected_row_count", "int64"),
    ("transferred_file_count", "int64"),
    ("staged_row_count", "int64"),
    ("merged_row_count", "int64"),
    ("deletes_applied", "int64"),
    ("delete_strategy", "string"),
    ("watermark", "timestamp"),
    ("started_at", "timestamp"),
    ("finished_at", "timestamp"),
    ("duration_seconds", "float64"),
    ("error_message", "string"),
)


def build_create_audit_table(cfg: PipelineConfig, d: Dialect = BIGQUERY) -> str:
    """Audit table: one row per table per pipeline run attempt (append-only)."""
    defs = [f"{d.ident(n)} {d.type(t)}" for n, t in AUDIT_COLUMNS]
    sql = f"CREATE TABLE IF NOT EXISTS {audit_table_ref(cfg, d)} (\n  " + ",\n  ".join(defs) + "\n)"
    if d.name == "bigquery":
        sql += (
            "\nPARTITION BY DATE(started_at)\nCLUSTER BY table_name, status"
            "\nOPTIONS (description = 'CDC pipeline audit log (append-only)')"
        )
    return sql


def build_audit_insert(cfg: PipelineConfig, d: Dialect = BIGQUERY) -> str:
    """Parameterised INSERT of one audit row (NULL params are CAST for type safety)."""
    names = [n for n, _ in AUDIT_COLUMNS]
    values = [f"CAST({d.param(n)} AS {d.type(t)})" for n, t in AUDIT_COLUMNS]
    return (
        f"INSERT INTO {audit_table_ref(cfg, d)} ({', '.join(d.ident(n) for n in names)})\n"
        f"VALUES ({', '.join(values)})"
    )


def build_all_ddl(cfg: PipelineConfig, d: Dialect = BIGQUERY) -> list[str]:
    """Every DDL statement, in dependency order."""
    stmts = build_create_datasets(cfg, d)
    for t in cfg.tables:
        stmts += [build_create_staging_table(cfg, t, d), build_create_final_table(cfg, t, d)]
    stmts.append(build_create_audit_table(cfg, d))
    return stmts
