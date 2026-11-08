"""MERGE / DELETE / DDL generation."""

from __future__ import annotations

import pytest

from bigquery import sql_builder as sb
from bigquery.scripts.deploy_bigquery import SQL_DIR, render_files


def test_merge_upserts_shape(default_cfg):
    sql = sb.build_merge_upserts(default_cfg, default_cfg.table("orders"))
    assert sql.startswith("MERGE INTO `your-gcp-project-id.analytics.orders` AS T")
    assert "FROM `your-gcp-project-id.cdc_staging.orders_cdc_stage`" in sql
    assert "WHERE `_pipeline_run_id` = @run_id" in sql
    assert "`_cdc_operation` IN ('INSERT', 'UPDATE')" in sql
    assert "QUALIFY ROW_NUMBER() OVER" in sql
    assert "ON T.`order_id` = S.`order_id`" in sql
    assert "S.`_source_sequence` > COALESCE(T.`_source_sequence`, '')" in sql
    assert "`_is_deleted` = FALSE" in sql
    assert "SELECT *" not in sql


def test_soft_delete_only_matched_and_never_inserts(default_cfg):
    sql = sb.build_apply_deletes(default_cfg, default_cfg.table("orders"), "soft")
    assert "`_cdc_operation` IN ('DELETE')" in sql
    assert "`_is_deleted` = TRUE" in sql
    assert "NOT MATCHED" not in sql


def test_hard_delete(default_cfg):
    sql = sb.build_apply_deletes(default_cfg, default_cfg.table("customers"), "hard")
    assert sql.rstrip().endswith("THEN\n  DELETE")


def test_unknown_delete_strategy(default_cfg):
    with pytest.raises(ValueError):
        sb.build_apply_deletes(default_cfg, default_cfg.table("orders"), "purge")


def test_duckdb_dialect(default_cfg):
    sql = sb.build_merge_upserts(default_cfg, default_cfg.table("orders"), sb.DUCKDB)
    assert 'MERGE INTO "analytics"."orders" AS T' in sql and "$run_id" in sql
    assert "current_timestamp" in sql and "`" not in sql


def test_ddl_partitioning_and_clustering(default_cfg):
    orders = sb.build_create_final_table(default_cfg, default_cfg.table("orders"))
    customers = sb.build_create_final_table(default_cfg, default_cfg.table("customers"))
    assert "PARTITION BY `order_date`" in orders
    assert "PARTITION BY DATE(`created_at`)" in customers
    assert "CLUSTER BY `customer_id`, `order_id`" in orders
    assert "PRIMARY KEY (`order_id`) NOT ENFORCED" in orders
    assert "`total_amount` NUMERIC(12,2)" in orders
    stage = sb.build_create_staging_table(default_cfg, default_cfg.table("orders"))
    assert "`status` STRING," in stage  # only the PK is NOT NULL in staging


def test_rejects_identifier_injection():
    with pytest.raises(ValueError):
        sb.BIGQUERY.ident("x`; DROP TABLE y; --")


def test_committed_sql_files_match_generator(default_cfg):
    """bigquery/sql/*.sql must be regenerated when config or builder changes."""
    for name, text in render_files(default_cfg).items():
        assert (SQL_DIR / name).read_text() == text, f"run `make sql` ({name} is stale)"
