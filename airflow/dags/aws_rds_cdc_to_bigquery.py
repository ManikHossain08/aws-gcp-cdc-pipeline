"""aws_rds_cdc_to_bigquery — RDS PostgreSQL CDC (DMS -> S3 -> Glue) into BigQuery.

check_new_cdc_files >> run_glue_cdc_job >> validate_glue_output >> transfer_s3_to_gcs
  >> load_gcs_to_bigquery_staging >> merge_staging_to_final >> apply_deletes
  >> update_watermark >> publish_audit_metrics (runs on success AND failure)

Params (Trigger DAG w/ config):
  tables           list of table names (default: all configured tables)
  ingest_date      YYYY-MM-DD -> backfill every raw file of that date (watermark ignored)
  run_id           reprocess an existing run id exactly (reuses its input manifest)
  delete_strategy  soft | hard (overrides Airflow Variable cdc_delete_strategy / env)

Safe to rerun: the run id is derived from the logical date, every task overwrites only
run-scoped outputs, and the BigQuery MERGE applies each change at most once.
"""

from __future__ import annotations

import os
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow import __version__ as AIRFLOW_VERSION
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule

REPO_ROOT = Path(__file__).resolve().parents[2]
for extra in (REPO_ROOT, REPO_ROOT / "airflow" / "plugins"):
    if str(extra) not in sys.path:
        sys.path.insert(0, str(extra))

import cdc_operators as ops  # noqa: E402
from cdc_common.paths import build_run_id  # noqa: E402

DAG_ID = "aws_rds_cdc_to_bigquery"
AIRFLOW_2 = AIRFLOW_VERSION.startswith("2.")
TASK_IDS = [
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


def _alert_on_failure(context: dict[str, Any]) -> None:
    ti = context["task_instance"]
    ops.notify_slack(
        f":red_circle: {DAG_ID}.{ti.task_id} failed (try {ti.try_number}) "
        f"run={context['run_id']} log={ti.log_url}"
    )


def _sla_miss(*args: Any, **kwargs: Any) -> None:
    ops.notify_slack(f":warning: {DAG_ID} SLA missed")


default_args: dict[str, Any] = {
    "owner": "data-platform",
    "retries": int(os.environ.get("CDC_TASK_RETRIES", "3")),
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "execution_timeout": timedelta(hours=1),
    "on_failure_callback": _alert_on_failure,
    "email": [e for e in os.environ.get("ALERT_EMAILS", "").split(",") if e],
    "email_on_failure": bool(os.environ.get("ALERT_EMAILS")),
    "email_on_retry": False,
}
dag_kwargs: dict[str, Any] = {}
if AIRFLOW_2:  # SLAs were removed in Airflow 3 (use Deadline Alerts there)
    default_args["sla"] = timedelta(minutes=int(os.environ.get("CDC_SLA_MINUTES", "90")))
    dag_kwargs["sla_miss_callback"] = _sla_miss


def _overrides(params: dict[str, Any]) -> dict[str, str]:
    strategy = params.get("delete_strategy") or Variable.get("cdc_delete_strategy", default_var="")
    return {"CDC_DELETE_STRATEGY": strategy} if strategy else {}


def _cfg(context: dict[str, Any]) -> Any:
    return ops.get_config(_overrides(context["params"]))


def _run_id(context: dict[str, Any]) -> str:
    explicit = context["params"].get("run_id")
    if explicit:
        return str(explicit)
    when = context.get("logical_date") or context["dag_run"].run_after
    return build_run_id(when)


@dag(
    dag_id=DAG_ID,
    description="CDC from AWS RDS PostgreSQL via DMS/S3/Glue into BigQuery (MERGE)",
    schedule=os.environ.get("CDC_DAG_SCHEDULE", "0 * * * *"),
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # staging tables and watermarks assume a single writer
    default_args=default_args,
    tags=["cdc", "aws", "gcp", "bigquery"],
    params={
        "tables": Param([], type="array", description="Tables to process (empty = all)"),
        "ingest_date": Param(None, type=["null", "string"], description="Backfill YYYY-MM-DD"),
        "run_id": Param(None, type=["null", "string"], description="Reprocess this run id"),
        "delete_strategy": Param(None, type=["null", "string"], enum=[None, "soft", "hard"]),
    },
    render_template_as_native_obj=True,
    **dag_kwargs,
)
def aws_rds_cdc_to_bigquery() -> None:
    """Build the DAG."""

    @task(task_id="check_new_cdc_files")
    def check_new_cdc_files(**context: Any) -> dict[str, Any]:
        params = context["params"]
        result = ops.check_new_cdc_files(
            _cfg(context), _run_id(context), params.get("tables") or None, params.get("ingest_date")
        )
        result["started_at"] = pendulum.now("UTC").isoformat()
        return result

    @task(task_id="run_glue_cdc_job")
    def run_glue_cdc_job(check: dict[str, Any], **context: Any) -> dict[str, Any]:
        if not check["has_data"]:
            raise AirflowSkipException("No new CDC files since the last watermark")
        return ops.run_glue_cdc_job(_cfg(context), check["run_id"], list(check["files"]))

    @task(task_id="validate_glue_output")
    def validate_glue_output(
        check: dict[str, Any], _glue: dict[str, Any], **context: Any
    ) -> dict[str, Any]:
        return ops.validate_glue_output(_cfg(context), check["run_id"], list(check["files"]))

    @task(task_id="transfer_s3_to_gcs")
    def transfer_s3_to_gcs(
        check: dict[str, Any], _valid: dict[str, Any], **context: Any
    ) -> dict[str, Any]:
        return ops.transfer_s3_to_gcs(_cfg(context), check["run_id"], list(check["files"]))

    @task(task_id="load_gcs_to_bigquery_staging")
    def load_staging(
        check: dict[str, Any], _transfer: dict[str, Any], **context: Any
    ) -> dict[str, int]:
        return ops.load_gcs_to_bigquery_staging(
            _cfg(context), check["run_id"], list(check["files"])
        )

    @task(task_id="merge_staging_to_final")
    def merge(check: dict[str, Any], _staged: dict[str, int], **context: Any) -> dict[str, int]:
        return ops.merge_staging_to_final(_cfg(context), check["run_id"], list(check["files"]))

    @task(task_id="apply_deletes")
    def deletes(check: dict[str, Any], _merged: dict[str, int], **context: Any) -> dict[str, int]:
        return ops.apply_deletes(_cfg(context), check["run_id"], list(check["files"]))

    @task(task_id="update_watermark")
    def watermark(
        check: dict[str, Any], _deleted: dict[str, int], **context: Any
    ) -> dict[str, str]:
        return ops.update_watermark(_cfg(context), check["run_id"])

    @task(task_id="publish_audit_metrics", trigger_rule=TriggerRule.ALL_DONE, retries=1)
    def publish(**context: Any) -> None:
        ti, dag_run = context["ti"], context["dag_run"]
        check = ti.xcom_pull(task_ids="check_new_cdc_files")
        if not check:
            raise RuntimeError("check_new_cdc_files produced no result; nothing to audit")
        states = {
            t.task_id: t.state
            for t in dag_run.get_task_instances()
            if t.task_id != "publish_audit_metrics"
        }
        failed = sorted(k for k, v in states.items() if v in {"failed", "upstream_failed"})
        status = "FAILED" if failed else ("SUCCEEDED" if check["has_data"] else "NO_DATA")
        ops.publish_audit_metrics(
            _cfg(context),
            check["run_id"],
            dag_run.run_id,
            status,
            check["started_at"],
            {
                "check": check,
                "staged": ti.xcom_pull(task_ids="load_gcs_to_bigquery_staging"),
                "merged": ti.xcom_pull(task_ids="merge_staging_to_final"),
                "deleted": ti.xcom_pull(task_ids="apply_deletes"),
            },
            error_message=f"failed tasks: {failed}" if failed else None,
        )
        if failed:
            raise RuntimeError(f"Pipeline run failed in {failed}")  # keep the DAG run red

    check = check_new_cdc_files()
    glue = run_glue_cdc_job(check)
    valid = validate_glue_output(check, glue)
    transferred = transfer_s3_to_gcs(check, valid)
    staged = load_staging(check, transferred)
    merged = merge(check, staged)
    deleted = deletes(check, merged)
    wm = watermark(check, deleted)
    wm >> publish()


aws_rds_cdc_to_bigquery()
