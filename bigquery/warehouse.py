"""Warehouse adapters: Google BigQuery (production) and DuckDB (local stand-in)."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse

from bigquery.sql_builder import BIGQUERY, DUCKDB, Dialect
from cdc_common.config import PipelineConfig

JOB_LABELS = {"pipeline": "aws-rds-cdc", "component": "bq-loader"}


class Warehouse(Protocol):
    """What the loader needs from a warehouse."""

    dialect: Dialect

    def execute(
        self, sql: str, params: dict[str, Any] | None = None
    ) -> tuple[list[dict[str, Any]], int]:
        """Run a statement; return (rows, affected-row count for DML)."""

    def load_parquet(self, uris: list[str], dataset: str, table: str) -> None:
        """Replace the table's content with the given Parquet files."""


class BigQueryWarehouse:
    """google-cloud-bigquery implementation."""

    dialect = BIGQUERY

    def __init__(self, project: str, location: str, client: Any | None = None) -> None:
        if client is None:
            from google.cloud import bigquery

            client = bigquery.Client(project=project, location=location)
        self.client = client
        self.project = project
        self.location = location

    @staticmethod
    def _param(name: str, value: Any) -> Any:
        from google.cloud import bigquery

        if isinstance(value, bool):
            typ = "BOOL"
        elif isinstance(value, int):
            typ = "INT64"
        elif isinstance(value, float):
            typ = "FLOAT64"
        elif isinstance(value, Decimal):
            typ = "NUMERIC"
        elif isinstance(value, datetime):
            typ = "TIMESTAMP"
        elif isinstance(value, date):
            typ = "DATE"
        else:
            typ = "STRING"  # also None -> typed NULL, SQL CASTs to the column type
        return bigquery.ScalarQueryParameter(name, typ, value)

    def execute(
        self, sql: str, params: dict[str, Any] | None = None
    ) -> tuple[list[dict[str, Any]], int]:
        """Run a query job with named parameters and job labels."""
        from google.cloud import bigquery

        config = bigquery.QueryJobConfig(
            query_parameters=[self._param(k, v) for k, v in (params or {}).items()],
            labels=JOB_LABELS,
        )
        job = self.client.query(sql, job_config=config, location=self.location)
        rows = [dict(r.items()) for r in job.result()]
        return rows, int(job.num_dml_affected_rows or 0)

    def load_parquet(self, uris: list[str], dataset: str, table: str) -> None:
        """Load job with WRITE_TRUNCATE into the pre-created staging table."""
        from google.cloud import bigquery

        config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
            decimal_target_types=["NUMERIC", "BIGNUMERIC"],
            labels=JOB_LABELS,
        )
        ref = f"{self.project}.{dataset}.{table}"
        self.client.load_table_from_uri(uris, ref, job_config=config).result()


class DuckDBWarehouse:
    """Local stand-in executing the same SQL (DuckDB dialect) against a file database."""

    dialect = DUCKDB

    def __init__(self, path: str) -> None:
        import duckdb

        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.conn = duckdb.connect(path)
        self.conn.execute("SET TimeZone = 'UTC'")

    def execute(
        self, sql: str, params: dict[str, Any] | None = None
    ) -> tuple[list[dict[str, Any]], int]:
        """Run SQL with ``$name`` parameters; DML returns the affected-row count."""
        used = {k: v for k, v in (params or {}).items() if f"${k}" in sql}
        cur = self.conn.execute(sql, used or None)
        if cur.description is None:
            return [], 0
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]
        if sql.lstrip().upper().startswith(("MERGE", "INSERT", "UPDATE", "DELETE")) and cols == [
            "Count"
        ]:
            return [], int(rows[0]["Count"]) if rows else 0
        return rows, 0

    def load_parquet(self, uris: list[str], dataset: str, table: str) -> None:
        """Truncate then INSERT BY NAME from local Parquet files (mirrors WRITE_TRUNCATE)."""
        local = [urlparse(u).path if u.startswith("file://") else u for u in uris]
        target = f'"{dataset}"."{table}"'
        self.conn.execute("BEGIN")
        try:
            self.conn.execute(f"DELETE FROM {target}")  # noqa: S608
            if local:
                self.conn.execute(
                    f"INSERT INTO {target} BY NAME SELECT * FROM read_parquet(?, hive_partitioning = false)",
                    [local],
                )
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    def close(self) -> None:
        """Close the database file (DuckDB allows one writer process)."""
        self.conn.close()


def warehouse_for(cfg: PipelineConfig) -> BigQueryWarehouse | DuckDBWarehouse:
    """Warehouse adapter for the configured backend."""
    if cfg.bq_backend == "bigquery":
        return BigQueryWarehouse(cfg.bq_project, cfg.bq_location)
    return DuckDBWarehouse(cfg.duckdb_path)
