# Operations runbook

Every log line is JSON and carries `pipeline_run_id`, so you can search one run across Glue
(CloudWatch), the Airflow task logs and BigQuery job labels (`pipeline=aws-rds-cdc`).

## Monitoring

| Signal | Where | Alert |
|---|---|---|
| DAG/task failures, SLA misses | Airflow; Slack webhook (`SLACK_WEBHOOK_URL`); email (`ALERT_EMAILS`) | `on_failure_callback`, `sla_miss_callback` (Airflow 2) |
| DMS latency | CloudWatch `CDCLatencySource`/`CDCLatencyTarget` | SNS `cdc-<env>-cdc-alerts` |
| Replication slot lag / WAL disk | CloudWatch `OldestReplicationSlotLag`, `TransactionLogsDiskUsage` | SNS |
| DMS task state changes, Glue job failures | EventBridge rules | SNS |
| Pipeline health | `cdc_audit.v_pipeline_health`, `bigquery/sql/dashboard_queries.sql` | Looker Studio |

Daily health check:
```sql
SELECT * FROM `your-gcp-project-id.cdc_audit.v_pipeline_health`
WHERE run_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
```

## Restart from a failed run

1. Find the failed task in Airflow. The task's log shows `pipeline_run_id`.
2. Fix the cause (see the table below).
3. **Clear** the failed task, or the whole DAG run. The run id is derived from the logical
   date, so the rerun uses the same input files and overwrites the same run-scoped outputs.
   The MERGE ignores changes it has already applied.
4. To replay an older run explicitly, trigger with `{"run_id": "cdc_20261105T100000Z"}`.

The watermark advances only after `apply_deletes` succeeds, so a failed run never skips data.

## Replication slot lag

Symptoms: the `OldestReplicationSlotLag` / `TransactionLogsDiskUsage` alarms fire, or RDS free
storage drops.

1. `aws dms describe-replication-tasks`: is the task running? Read `StopReason`.
2. On RDS:
   ```sql
   SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn))
   FROM pg_replication_slots;
   ```
3. If the task is stopped, resume it with
   `aws dms start-replication-task --start-replication-task-type resume-processing`.
4. If it's running but slow, look for long-running transactions on the source (they hold the
   slot), check the DMS instance CPU and memory, and scale the instance class.
5. **Last resort** (the disk is about to fill): stop the task, drop the slot, recreate it, and
   do a full reload of the affected tables. The MERGE handles reload rows: snapshot rows lose
   against newer CDC rows that are already applied. Rows deleted at source while the slot was
   gone aren't in the reload, so mark them afterwards:
   ```sql
   UPDATE `your-gcp-project-id.analytics.orders` SET _is_deleted = TRUE, _loaded_at = CURRENT_TIMESTAMP()
   WHERE NOT _is_deleted AND _loaded_at < TIMESTAMP('<reload start>');
   ```
   Run this only after the reload batch has been applied.

## Reprocess a date partition

```bash
airflow dags trigger aws_rds_cdc_to_bigquery -c '{"ingest_date": "2026-11-04", "tables": ["orders"]}'
```

This lists every raw file of that date (the watermark is ignored and never moves backwards),
reprocesses them, and lets the MERGE skip anything already applied. It's safe at any time.

## Schema evolution

The contract is `config/pipeline_config.yaml`.

* **Add a nullable column at source**
  1. Add it to `pipeline_config.yaml` in a PR.
  2. Run `make sql`.
  3. Run `ALTER TABLE … ADD COLUMN` on the final and staging tables
     (`deploy_bigquery.py --apply` creates missing tables but doesn't alter existing ones).
  4. Deploy the artifacts (`make deploy-aws` uploads the config).

  Until then, Glue ignores the extra source column. After deploying, backfill the dates that
  carried the column if you need historical values.
* **Type change / rename / drop:** a planned migration.
  1. Add a new column.
  2. Backfill it via `ingest_date` reprocessing.
  3. Move consumers to it.
  4. Drop the old column.

  An in-place type change on the final table needs `CREATE TABLE … AS SELECT CAST(…)` and a swap.
* **New table:** add it to the config and to `dms/table_mappings.json`, grant `SELECT` to
  `dms_cdc_user`, set `REPLICA IDENTITY FULL`, then re-apply Terraform. DMS does a full load
  of the new table.

## Investigate rejected records

Rejected rows sit in `s3://<processed>/rejected/table=<t>/run_id=<r>/ingest_date=<d>/`. They're
JSON lines with `_errors`, `_raw_record` and `_source_file`.

```bash
aws s3 cp --recursive s3://your-processed-bucket-name/rejected/table=orders/run_id=cdc_20261105T100000Z/ ./rejected/
```

```bash
cat rejected/*/part-*.json | jq -r '._errors[]' | sort | uniq -c
```

Typical errors: `invalid_type:<col>` (a source type change; update the contract),
`missing_primary_key`, `invalid_operation`, `malformed_record` (a truncated file).
`max_rejected_ratio` (default 5%) controls when the DAG stops instead of continuing.

## Rotate credentials

| Credential | How |
|---|---|
| RDS admin | Managed by RDS in Secrets Manager (`manage_master_user_password`); rotation is built in |
| DMS user | Re-run `scripts/bootstrap_aws.sh` (sets a new password and updates the secret), then `aws dms test-connection` |
| GCP access from AWS | WIF has no keys to rotate. With the SA-key fallback: create a new key, `put-secret-value`, then delete the old key |
| Airflow → GCP/AWS | Use workload identity or IAM roles; no static keys |
| Slack webhook | Rotate in Slack; update the Airflow secret backend |

## Common errors

| Error | Cause | Fix |
|---|---|---|
| `GlueOutputValidationError: rejected ratio …` | Bad source data or contract drift | Inspect `rejected/`, fix, rerun |
| `StagingCountMismatchError` | Files missing in GCS or a partial load | Rerun `transfer_s3_to_gcs`, then the load |
| `ChecksumMismatchError` | A processed file changed after Glue wrote the manifest | Rerun `run_glue_cdc_job` for the run |
| DMS `Could not find slot` | Slot not created or dropped | Run `03_enable_cdc.sql`, or remove `slot_name` so DMS creates one |
| BigQuery `Query error: … NOT NULL` on load | Staging NOT NULL on a non-PK column | Staging allows NULLs except on the PK; redeploy the DDL from `make sql` |
| Airflow `PYTHON_VERSION_MISMATCH` (local Spark) | Spark workers using another Python | Set `PYSPARK_PYTHON` to the venv Python (done automatically) |

More in [docs/troubleshooting.md](docs/troubleshooting.md).
