"""S3 -> GCS transfer logic: manifest scoping, checksums, idempotency, S3 via moto."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from cdc_common import paths
from cdc_common.manifest import GlueAuditManifest, OutputFile
from cdc_common.storage import LocalObjectStore, S3ObjectStore, read_json, write_json
from transfer.s3_to_gcs_transfer import (
    ChecksumMismatchError,
    read_transfer_result,
    transfer_run,
)

RUN = "cdc_20261105T090000Z"


def put(store, key: str, data: bytes) -> OutputFile:
    store.write_bytes(key, data)
    return OutputFile(key=key, size=len(data), md5=hashlib.md5(data).hexdigest())  # noqa: S324


def make_run(cfg, src, status="SUCCEEDED") -> GlueAuditManifest:
    up = put(
        src, paths.deduplicated_prefix(cfg, "orders", RUN, "upserts") + "part-0.parquet", b"U" * 100
    )
    de = put(
        src, paths.deduplicated_prefix(cfg, "orders", RUN, "deletes") + "part-0.parquet", b"D" * 10
    )
    # A rejected file and another run's file exist but are NOT in the manifest.
    put(src, paths.rejected_prefix(cfg, "orders", RUN) + "part-0.json", b"bad")
    put(
        src,
        paths.deduplicated_prefix(cfg, "orders", "cdc_20261104T090000Z", "upserts") + "x.parquet",
        b"old",
    )
    manifest = GlueAuditManifest(
        pipeline_run_id=RUN,
        table="orders",
        status=status,
        input_row_count=2,
        valid_row_count=2,
        upsert_row_count=1,
        delete_row_count=1,
        outputs={"upserts": [up], "deletes": [de]},
    )
    write_json(src, paths.glue_manifest_key(cfg, "orders", RUN), manifest.to_dict())
    return manifest


@pytest.fixture
def stores(tmp_path: Path):
    return LocalObjectStore(tmp_path / "processed"), LocalObjectStore(tmp_path / "gcs")


def test_transfers_only_manifest_files(local_cfg, stores):
    src, dest = stores
    make_run(local_cfg, src)
    [result] = transfer_run(local_cfg, RUN, src, dest)
    keys = sorted(o.key for o in dest.list(""))
    assert keys == [
        f"cdc/orders/run_id={RUN}/deletes/part-0.parquet",
        f"cdc/orders/run_id={RUN}/upserts/part-0.parquet",
    ]
    assert result.status == "SUCCEEDED" and result.bytes_copied == 110
    assert {f.action for f in result.files} == {"copied"}
    assert len(result.uris("upserts")) == 1


def test_rerun_skips_identical_files(local_cfg, stores):
    src, dest = stores
    make_run(local_cfg, src)
    transfer_run(local_cfg, RUN, src, dest)
    [again] = transfer_run(local_cfg, RUN, src, dest)
    assert {f.action for f in again.files} == {"skipped_identical"}
    assert again.bytes_copied == 0


def test_checksum_mismatch_fails_and_is_recorded(local_cfg, stores):
    src, dest = stores
    manifest = make_run(local_cfg, src)
    src.write_bytes(manifest.outputs["upserts"][0].key, b"tampered")  # changed after Glue
    with pytest.raises(ChecksumMismatchError):
        transfer_run(local_cfg, RUN, src, dest)
    result = read_transfer_result(local_cfg, src, "orders", RUN)
    assert result is not None and result.status == "FAILED"
    assert not dest.list("cdc/orders/") or all("upserts" not in o.key for o in dest.list(""))


def test_failed_glue_manifest_is_not_transferred(local_cfg, stores):
    src, dest = stores
    make_run(local_cfg, src, status="FAILED")
    with pytest.raises(RuntimeError, match="refusing"):
        transfer_run(local_cfg, RUN, src, dest)
    assert dest.list("") == []


def test_tables_without_manifest_are_skipped(local_cfg, stores):
    src, dest = stores
    assert transfer_run(local_cfg, RUN, src, dest) == []


def test_transfer_from_s3_with_moto(local_cfg, tmp_path):
    moto = pytest.importorskip("moto")
    import boto3

    with moto.mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="your-processed-bucket-name")
        src = S3ObjectStore("your-processed-bucket-name", client)
        dest = LocalObjectStore(tmp_path / "gcs")
        make_run(local_cfg, src)
        [result] = transfer_run(local_cfg, RUN, src, dest)
        assert result.status == "SUCCEEDED" and len(result.files) == 2
        stored = read_json(src, paths.transfer_result_key(local_cfg, "orders", RUN))
        assert stored["status"] == "SUCCEEDED"


def test_local_store_rejects_path_traversal(tmp_path):
    store = LocalObjectStore(tmp_path / "bucket")
    with pytest.raises(ValueError):
        store.write_bytes("../escape.txt", b"x")
