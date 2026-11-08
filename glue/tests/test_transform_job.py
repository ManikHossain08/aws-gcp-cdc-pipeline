"""End-to-end Glue transform on fixtures against local buckets."""

from __future__ import annotations

import pytest

from cdc_common import paths
from cdc_common.storage import read_json, spark_path, store_for_uri, write_json
from conftest import stage_fixture
from glue.cdc_transform_job import run_transform
from glue.utils.audit import read_manifest

pytestmark = pytest.mark.spark
RUN = "cdc_20261105T090000Z"
DAY = "cdc/public/orders/2026/11/05/"


def prepare(cfg, fixtures):
    keys = [
        stage_fixture(cfg, f, f"{DAY}2026110508{i:02d}00000.jsonl") for i, f in enumerate(fixtures)
    ]
    store = store_for_uri(cfg.processed_uri)
    write_json(
        store,
        paths.input_manifest_key(cfg, RUN),
        {"run_id": RUN, "tables": {"orders": [{"key": k} for k in keys]}},
    )
    return store


def test_transform_writes_all_zones_and_consistent_manifest(spark, local_cfg):
    store = prepare(
        local_cfg,
        [
            "insert.jsonl",
            "update.jsonl",
            "delete.jsonl",
            "duplicates.jsonl",
            "out_of_order.jsonl",
            "malformed.jsonl",
        ],
    )
    [manifest] = run_transform(spark, local_cfg, RUN, rows_per_file=1000)

    assert manifest.status == "SUCCEEDED"
    assert manifest.input_file_count == 6
    assert manifest.input_row_count == 13
    assert manifest.rejected_row_count == 5
    assert manifest.valid_row_count == 8
    assert manifest.upsert_row_count == 1  # order 201 (latest: DELIVERED)
    assert manifest.delete_row_count == 1  # order 202
    assert manifest.duplicate_row_count == 6
    assert manifest.check_consistency() == []
    assert manifest.outputs["upserts"] and manifest.outputs["deletes"]
    assert all(len(f.md5) == 32 for f in manifest.outputs["upserts"])

    # persisted manifest == returned manifest
    assert read_manifest(local_cfg, store, "orders", RUN) == manifest
    rejected = store.list(paths.rejected_prefix(local_cfg, "orders", RUN))
    assert any(o.key.endswith(".json") for o in rejected)
    validated = store.list(paths.validated_prefix(local_cfg, "orders", RUN))
    assert any("ingest_date=2026-11-05" in o.key for o in validated)


def test_latest_state_respects_out_of_order_delivery(spark, local_cfg):
    prepare(local_cfg, ["insert.jsonl", "out_of_order.jsonl"])
    run_transform(spark, local_cfg, RUN)
    upserts = spark.read.parquet(
        spark_path(
            local_cfg.processed_uri, paths.deduplicated_prefix(local_cfg, "orders", RUN, "upserts")
        )
    )
    status = {r["order_id"]: r["status"] for r in upserts.collect()}
    assert status == {201: "DELIVERED", 202: "NEW"}


def test_rerun_same_run_id_is_idempotent(spark, local_cfg):
    store = prepare(local_cfg, ["insert.jsonl", "update.jsonl"])
    first = run_transform(spark, local_cfg, RUN)[0]
    second = run_transform(spark, local_cfg, RUN)[0]
    counts = lambda mf: {k: v for k, v in mf.to_dict().items() if k.endswith("_count")}  # noqa: E731
    assert counts(first) == counts(second)
    # overwrite: still exactly the files of one run (no accumulation)
    files = [
        o
        for o in store.list(paths.deduplicated_prefix(local_cfg, "orders", RUN, "upserts"))
        if o.key.endswith(".parquet")
    ]
    assert len(files) == len(second.outputs["upserts"])


def test_rejects_malformed_run_id(spark, local_cfg):
    with pytest.raises(ValueError, match="Malformed run_id"):
        run_transform(spark, local_cfg, "../../etc")


def test_input_manifest_is_required(spark, local_cfg):
    with pytest.raises(FileNotFoundError):
        read_json(store_for_uri(local_cfg.processed_uri), paths.input_manifest_key(local_cfg, RUN))
