"""Task-layer behaviour: replayable input manifests, validation gates, Glue polling."""

from __future__ import annotations

import pytest

import cdc_operators as ops
from cdc_common import paths
from cdc_common.manifest import GlueAuditManifest, OutputFile
from cdc_common.storage import store_for_uri, write_json
from conftest import stage_fixture

RUN = "cdc_20261105T090000Z"


def test_check_new_files_persists_and_reuses_manifest(local_cfg):
    stage_fixture(local_cfg, "insert.jsonl", "cdc/public/orders/2026/11/05/a.jsonl")
    first = ops.check_new_cdc_files(local_cfg, RUN)
    assert first == {"run_id": RUN, "files": {"orders": 1}, "has_data": True, "mode": "incremental"}
    stage_fixture(local_cfg, "update.jsonl", "cdc/public/orders/2026/11/05/b.jsonl")
    # same run id => same files (exact replay), even though a new file arrived
    assert ops.check_new_cdc_files(local_cfg, RUN)["files"] == {"orders": 1}
    assert ops.check_new_cdc_files(local_cfg, "cdc_20261105T100000Z")["files"] == {"orders": 2}


def test_backfill_by_ingest_date(local_cfg):
    stage_fixture(local_cfg, "insert.jsonl", "cdc/public/orders/2026/11/04/a.jsonl")
    stage_fixture(local_cfg, "update.jsonl", "cdc/public/orders/2026/11/05/b.jsonl")
    out = ops.check_new_cdc_files(local_cfg, RUN, ingest_date="2026-11-04")
    assert out["files"] == {"orders": 1} and out["mode"] == "backfill"


def test_no_files_means_no_data(local_cfg):
    assert ops.check_new_cdc_files(local_cfg, RUN)["has_data"] is False


def _manifest(cfg, **counts):
    store = store_for_uri(cfg.processed_uri)
    f = OutputFile("deduplicated/x.parquet", 1, "0" * 32)
    m = GlueAuditManifest(RUN, "orders", "SUCCEEDED", outputs={"upserts": [f]}, **counts)
    write_json(store, paths.glue_manifest_key(cfg, "orders", RUN), m.to_dict())


def test_validate_rejects_missing_manifest(local_cfg):
    with pytest.raises(ops.GlueOutputValidationError, match="no Glue manifest"):
        ops.validate_glue_output(local_cfg, RUN, ["orders"])


def test_validate_enforces_rejection_threshold(local_cfg):
    _manifest(
        local_cfg, input_row_count=10, valid_row_count=1, rejected_row_count=9, upsert_row_count=1
    )
    with pytest.raises(ops.GlueOutputValidationError, match="rejected ratio"):
        ops.validate_glue_output(
            local_cfg.__class__(**{**local_cfg.__dict__, "max_rejected_ratio": 0.5}),
            RUN,
            ["orders"],
        )


def test_validate_passes_consistent_manifest(local_cfg):
    _manifest(local_cfg, input_row_count=2, valid_row_count=2, upsert_row_count=2)
    out = ops.validate_glue_output(local_cfg, RUN, ["orders"])
    assert out["tables"]["orders"]["upsert_row_count"] == 2


class FakeGlue:
    def __init__(self, states):
        self.states = list(states)
        self.started = []

    def start_job_run(self, **kw):
        self.started.append(kw)
        return {"JobRunId": "jr_1"}

    def get_job_run(self, **_):
        return {"JobRun": {"JobRunState": self.states.pop(0), "ErrorMessage": "boom"}}


def test_glue_polling_succeeds(local_cfg):
    glue = FakeGlue(["RUNNING", "RUNNING", "SUCCEEDED"])
    assert (
        ops._run_glue_job(local_cfg, "job", {"--run_id": RUN}, glue, sleep=lambda s: None) == "jr_1"
    )
    assert glue.started[0]["Arguments"] == {"--run_id": RUN}


def test_glue_polling_raises_on_failure(local_cfg):
    with pytest.raises(RuntimeError, match="FAILED: boom"):
        ops._run_glue_job(local_cfg, "job", {}, FakeGlue(["FAILED"]), sleep=lambda s: None)


def test_slack_is_noop_without_webhook(monkeypatch):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    assert ops.notify_slack("hello") is False
