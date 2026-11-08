"""DAG import, task ids, dependencies, retries and params."""

from __future__ import annotations

from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402

DAGS = Path(__file__).resolve().parents[1] / "dags"
DAG_ID = "aws_rds_cdc_to_bigquery"
CHAIN = [
    "check_new_cdc_files",
    "run_glue_cdc_job",
    "validate_glue_output",
    "transfer_s3_to_gcs",
    "load_gcs_to_bigquery_staging",
    "merge_staging_to_final",
    "apply_deletes",
    "update_watermark",
    "publish_audit_metrics",
]


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder=str(DAGS), include_examples=False)
    assert bag.import_errors == {}, bag.import_errors
    return bag.dags[DAG_ID]  # parsed DAG; no metadata-DB lookup needed


def test_dag_loads(dag):
    assert dag is not None
    assert dag.max_active_runs == 1
    assert dag.catchup is False


def test_all_tasks_present(dag):
    assert set(dag.task_ids) == set(CHAIN)


def test_linear_dependencies(dag):
    for upstream, downstream in zip(CHAIN, CHAIN[1:], strict=False):
        assert downstream in dag.get_task(upstream).downstream_task_ids, (upstream, downstream)


def test_retries_with_exponential_backoff(dag):
    for task in dag.tasks:
        if task.task_id == "publish_audit_metrics":
            continue
        assert task.retries >= 1
        assert task.retry_exponential_backoff is True


def test_audit_runs_even_on_failure(dag):
    assert dag.get_task("publish_audit_metrics").trigger_rule == "all_done"


def test_params_support_backfill(dag):
    assert {"tables", "ingest_date", "run_id", "delete_strategy"} <= set(dag.params.keys())


def test_failure_callback_configured(dag):
    assert all(t.on_failure_callback for t in dag.tasks)
