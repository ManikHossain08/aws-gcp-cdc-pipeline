# AWS DMS setup

Terraform (`infra/aws/dms.tf`) is the deployed source of truth. The JSON files in `dms/` serve
as reviewable references and work with the AWS CLI (`--cli-input-json`).

## Source: RDS PostgreSQL

* Parameter group: `rds.logical_replication=1` (needs a reboot), `wal_sender_timeout=0`,
  `max_replication_slots` / `max_wal_senders` ≥ 10, `rds.force_ssl=1`.
* `source_db/init/03_enable_cdc.sql` sets up:
  * a `dms_cdc_user` role with `rds_replication` and `SELECT` on the replicated tables;
  * `REPLICA IDENTITY FULL` on the replicated tables;
  * the `dms_cdc_publication` publication;
  * a pre-created logical replication slot, `dms_cdc_slot` (`test_decoding`).
* Endpoint (`dms_source_endpoint.json`): `SslMode=require`, credentials come from Secrets
  Manager, `SlotName=dms_cdc_slot`, `HeartbeatEnable=true` (keeps the slot advancing on idle
  databases), `CaptureDdls=false`.

  **A pre-created slot retains WAL from the moment it exists.** Start the task soon after
  creating it, or drop it.

## Target: S3

`dms_target_endpoint.json`:

| Setting | Value | Why |
|---|---|---|
| DataFormat | parquet (`parquet-2-0`, GZIP) | DMS S3 targets support CSV or Parquet (not JSON); Parquet is typed and compact |
| BucketFolder | `cdc` | `s3://<raw>/cdc/<schema>/<table>/…` |
| IncludeOpForFullLoad | true | Full-load rows carry `Op=I` |
| CdcInsertsAndUpdates | false | Deletes must be captured |
| TimestampColumnName | `_dms_ingest_ts` | Load time for full load; fallback ordering for snapshot rows |
| DatePartitionEnabled | true, `YYYYMMDD`, `SLASH` | CDC files in `YYYY/MM/DD/` folders (ingest-date partitioning) |
| CdcMaxBatchInterval / CdcMinFileSize | 60 s / 32 MB | Fewer, larger files (cost) at the price of up to 60 s extra latency |
| EncryptionMode | SSE_KMS (pipeline CMK) | Encryption at rest |

## Task: full load + CDC

`dms_replication_task.json` (settings) + `table_mappings.json` (selection and transformation
rules):

| Added column | Expression | Used as |
|---|---|---|
| `_dms_change_seq` | `$AR_H_CHANGE_SEQ` | Primary ordering key (35-digit monotonic sequence) |
| `_dms_commit_ts` | `$AR_H_COMMIT_TIMESTAMP` | `_cdc_timestamp` (secondary ordering, freshness) |
| `_dms_transaction_id` | `$AR_H_TRANSACTION_ID` | Lineage / debugging (where the source engine provides it) |
| `_dms_source_table` | `$AR_M_SOURCE_TABLE_NAME` | Lineage |

> **Verify before production.** Header and metadata variables (`AR_H_*`, `AR_M_*`) depend on
> the DMS engine version and the source. Look them up in "Transformation rule expressions" for
> your engine version, and run a short full-load-and-CDC test. Confirm in the first raw Parquet
> files that `_dms_change_seq` is populated for CDC rows and `_dms_commit_ts` for CDC rows. The
> Glue job rejects CDC rows that lack either. If `$AR_H_TRANSACTION_ID` isn't available, remove
> that rule; the column is optional.

Also confirm: `TargetTablePrepMode=DO_NOTHING` (never truncates the raw zone) and
`TableErrorPolicy=SUSPEND_TABLE` (one bad table doesn't stop the others).

## Operating DMS

* Start: `aws dms start-replication-task --replication-task-arn <arn> --start-replication-task-type start-replication`
* Resume after a stop: `--start-replication-task-type resume-processing`
* Reload one table: console → Table statistics → *Reload table data*. New `LOAD*.parquet`
  files then go through the normal pipeline.
* Metrics: `CDCLatencySource` and `CDCLatencyTarget` (alarms in `monitoring.tf`).
