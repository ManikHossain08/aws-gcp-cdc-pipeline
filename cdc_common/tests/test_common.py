"""Config, types, paths, watermark and storage helpers."""

from __future__ import annotations

import copy
from datetime import date, datetime, timedelta, timezone

import pytest
import yaml

from cdc_common import paths
from cdc_common.config import DEFAULT_CONFIG_PATH, ConfigError, parse_config, resolve_env
from cdc_common.manifest import GlueAuditManifest
from cdc_common.storage import LocalObjectStore
from cdc_common.types import UnsupportedTypeError, physical_types
from cdc_common.watermark import EPOCH, advance_watermark, discover_files, read_watermark


@pytest.fixture
def raw_config():
    return resolve_env(yaml.safe_load(DEFAULT_CONFIG_PATH.read_text()), env={})


def test_default_config_is_valid(default_cfg):
    assert [t.name for t in default_cfg.tables] == ["customers", "orders"]
    assert default_cfg.delete_strategy == "soft"
    assert default_cfg.raw_uri.startswith("file:///")


def test_env_interpolation():
    assert resolve_env("${A:-x}/${B:-y}", {"A": "1"}) == "1/y"


@pytest.mark.parametrize(
    ("path", "value", "error"),
    [
        (("pipeline", "delete_strategy"), "purge", "delete_strategy"),
        (("storage", "raw_format"), "csv", "raw_format"),
        (("bigquery", "backend"), "snowflake", "backend"),
    ],
)
def test_invalid_settings(raw_config, path, value, error):
    cfg = copy.deepcopy(raw_config)
    cfg[path[0]][path[1]] = value
    with pytest.raises(ConfigError, match=error):
        parse_config(cfg)


def test_primary_key_required(raw_config):
    cfg = copy.deepcopy(raw_config)
    cfg["tables"][0]["primary_key"] = []
    with pytest.raises(ConfigError, match="primary_key"):
        parse_config(cfg)


def test_reserved_column_prefix(raw_config):
    cfg = copy.deepcopy(raw_config)
    cfg["tables"][0]["columns"][1]["name"] = "_secret"
    with pytest.raises(ConfigError, match="reserved"):
        parse_config(cfg)


@pytest.mark.parametrize(
    ("logical", "bq"),
    [
        ("int64", "INT64"),
        ("decimal(12,2)", "NUMERIC(12,2)"),
        ("decimal(38,20)", "BIGNUMERIC(38,20)"),
        ("json", "STRING"),
        ("timestamp", "TIMESTAMP"),
    ],
)
def test_type_mapping(logical, bq):
    assert physical_types(logical).bigquery == bq


def test_unknown_type():
    with pytest.raises(UnsupportedTypeError):
        physical_types("geometry")


def test_run_id_and_key_helpers():
    run = paths.build_run_id(datetime(2026, 11, 5, 9, tzinfo=timezone.utc))
    assert run == "cdc_20261105T090000Z" and paths.validate_run_id(run)
    with pytest.raises(ValueError):
        paths.validate_run_id("cdc_../x")
    assert paths.ingest_date_from_key("cdc/public/orders/2026/11/05/a.parquet") == date(2026, 11, 5)
    assert paths.ingest_date_from_key("cdc/orders/ingest_date=2026-11-04/a.jsonl") == date(
        2026, 11, 4
    )
    assert paths.ingest_date_from_key("cdc/public/orders/LOAD00000001.parquet") is None
    assert paths.is_full_load_key("cdc/public/orders/LOAD00000001.parquet")


def test_watermark_is_monotonic(local_cfg):
    store = LocalObjectStore(__import__("pathlib").Path(local_cfg.processed_uri[7:]))
    assert read_watermark(local_cfg, store, "orders").last_modified == EPOCH
    t1 = datetime(2026, 11, 5, 9, tzinfo=timezone.utc)
    advance_watermark(local_cfg, store, "orders", t1, "cdc_20261105T090000Z")
    advance_watermark(local_cfg, store, "orders", t1 - timedelta(hours=1), "cdc_20261105T080000Z")
    assert read_watermark(local_cfg, store, "orders").last_modified == t1


def test_discovery_incremental_and_backfill(local_cfg, tmp_path):
    from pathlib import Path

    raw = LocalObjectStore(Path(local_cfg.raw_uri[7:]))
    table = local_cfg.table("orders")
    raw.write_bytes("cdc/public/orders/2026/11/04/a.jsonl", b"{}")
    raw.write_bytes("cdc/public/orders/2026/11/05/b.jsonl", b"{}")
    raw.write_bytes("cdc/public/orders/2026/11/05/ignore.txt", b"x")
    wm = read_watermark(local_cfg, LocalObjectStore(tmp_path / "p"), "orders")
    assert len(discover_files(local_cfg, raw, table, wm)) == 2
    future = wm.__class__("orders", datetime.now(timezone.utc) + timedelta(hours=1))
    assert discover_files(local_cfg, raw, table, future) == []
    backfill = discover_files(local_cfg, raw, table, future, date(2026, 11, 4))
    assert [f.key for f in backfill] == ["cdc/public/orders/2026/11/04/a.jsonl"]


def test_manifest_consistency_checks():
    good = GlueAuditManifest(
        "r",
        "orders",
        "SUCCEEDED",
        input_row_count=3,
        valid_row_count=2,
        rejected_row_count=1,
        upsert_row_count=2,
    )
    assert good.check_consistency() == ["upsert rows reported but no upsert files listed"]
    bad = GlueAuditManifest("r", "orders", "SUCCEEDED", input_row_count=5, valid_row_count=2)
    assert "input_row_count != valid_row_count + rejected_row_count" in bad.check_consistency()


def test_dms_table_mappings_cover_configured_tables(default_cfg):
    import json

    from cdc_common.config import REPO_ROOT

    rules = json.loads((REPO_ROOT / "dms" / "table_mappings.json").read_text())["rules"]
    selected = {
        (r["object-locator"]["schema-name"], r["object-locator"]["table-name"])
        for r in rules
        if r["rule-type"] == "selection" and r["rule-action"] == "include"
    }
    assert {(t.source_schema, t.name) for t in default_cfg.tables} <= selected
    added = {r["value"] for r in rules if r.get("rule-action") == "add-column"}
    f = default_cfg.cdc_fields
    assert {f.cdc_timestamp, f.sequence, f.transaction_id, f.source_table} <= added
