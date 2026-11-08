# Architecture

## 1. Goals and invariants

The pipeline must turn a stream of row-level changes from PostgreSQL into a BigQuery table
whose state equals the source state, with:

* **Identity:** one target row per source primary key. PKs are mandatory in config.
* **Ordering:** changes apply in source commit order, whatever order the files arrive in.
* **Idempotency:** applying the same change twice has no further effect, and re-running any
  task or whole DAG run is safe.
* **No silent loss:** every input row ends up in exactly one of validated, rejected or failed
  (and alerted), and the audit manifests prove the row counts add up.
* **Explicit deletes:** soft delete (default) or hard delete, applied only to matching keys.

## 2. Why each component

| Component | Why | Alternatives considered |
|---|---|---|
| **AWS DMS** (full load + CDC) | Managed log-based CDC from RDS PostgreSQL (logical replication slot). Does the initial snapshot and the ongoing changes in one task. Adds the ordering metadata (`AR_H_CHANGE_SEQ`). No source changes beyond logical replication. | Debezium on MSK/ECS (more control, more to operate); triggers (load on the source, not log-based). |
| **S3 raw zone** | Durable, cheap, immutable (versioned) record of every change. The replay source for any downstream bug. Decouples capture from transformation. | Kinesis/Kafka (lower latency, but retention and replay cost more). |
| **Glue 5.0 / PySpark** | Serverless Spark next to the data; scales with the batch; no cluster to run. Spark windows dedup by PK cheaply; explicit schemas and `try_cast` give record-level validation. | EMR (cheaper at large steady volume, more ops); Lambda (memory and time limits); dbt-in-BigQuery (moves raw data across clouds first). |
| **Curated hop S3→GCS** | Only validated, deduplicated, compressed files cross the cloud boundary. Smallest egress, and rejected data never leaves AWS. | Storage Transfer Service, BigQuery Omni, Data Transfer Service (§6). |
| **BigQuery MERGE** | Applies inserts and updates atomically per table, with conditions that enforce ordering. Idempotent by construction. | Append-only change table + views (simple writes, but every read pays for "latest row" logic); streaming upserts (not supported natively). |
| **Airflow** | Retries, backfills, SLAs, parameterised reruns and a cross-cloud view of each run in one DAG. Runs anywhere (MWAA, Composer, Astro, self-hosted). | Step Functions + Cloud Workflows (split control plane, one per cloud). |

## 3. Data flow and contracts

1. **DMS → raw.** DMS writes Parquet files to
   `cdc/<schema>/<table>/LOAD*.parquet` (full load) and `…/YYYY/MM/DD/<ts>.parquet` (CDC).
   Every row carries `Op` (I/U/D), `_dms_commit_ts`, `_dms_change_seq`,
   `_dms_transaction_id`, `_dms_source_table` and `_dms_ingest_ts`
   (see [dms/table_mappings.json](dms/table_mappings.json)).
   *Decision:* DMS can't write Hive-style `ingest_date=` folders, so the date-folder layout is
   kept and the ingest date is derived from the path. Processed zones use Hive-style
   partitions.
2. **check_new_cdc_files** lists raw files newer than the table's watermark minus an overlap
   window. It writes the exact list to `control/runs/run_id=<r>/input_manifest.json`. A rerun
   of the same `run_id` reuses that list.
3. **Glue** reads only those files, with an **explicit all-string schema**. It validates and
   types every row, then writes:
   `validated/` (all valid changes), `deduplicated/{upserts,deletes}/` (latest change per PK),
   `rejected/` (errors + original record), and finally `audit/…/glue_manifest.json`, which
   holds the counts and the per-file size and MD5. The manifest acts as the commit marker.
4. **validate_glue_output** checks that each manifest exists, has status SUCCEEDED, that its
   counts add up (`input = valid + rejected`, `valid = upserts + deletes + duplicates`), and
   that the rejection ratio is within the threshold.
5. **Transfer** copies **only manifest-listed files** to
   `gs://<staging>/cdc/<table>/run_id=<r>/{upserts,deletes}/`. Each file's MD5 is checked
   after download and again by GCS on upload. It writes `transfer_result.json`.
6. **Load** fills `cdc_staging.<table>_cdc_stage` with the transferred files (WRITE_TRUNCATE)
   and checks that the staged row count equals the manifest's upserts + deletes.
7. **MERGE** upserts, then **apply_deletes**, both filtered to `_pipeline_run_id = @run_id`.
8. **update_watermark** advances each table's watermark to the newest processed file.
   Watermarks only move forward. **publish_audit_metrics** writes one audit row per table
   and runs even when the DAG fails.

## 4. Ordering, duplicates and deletes

**Ordering key.** `(_source_sequence, _cdc_timestamp)`:

* `_source_sequence` is DMS `AR_H_CHANGE_SEQ`: unique, monotonic, fixed width (35 digits).
  Comparing the strings gives the same order as comparing the numbers. Commit timestamps alone
  are ambiguous, because many changes share one millisecond or one transaction.
* Full-load rows have no sequence, so they get `000…0`. Any CDC event for the same key
  therefore wins. Ties are broken by timestamp, then file name, so the result is deterministic.
* For full-load rows with no commit timestamp, DMS's `TimestampColumnName` (the load time) is
  used instead. A CDC row without a commit timestamp or sequence is rejected.

**Within a batch (Glue).** A window over the PK keeps the latest change, and this happens
*before* splitting into upserts and deletes. So INSERT→DELETE in one batch becomes one DELETE,
and DELETE→INSERT becomes one INSERT. Exact duplicates (DMS is at-least-once) collapse to one
row.

**Across batches (BigQuery).** Every MERGE action is guarded:

```sql
WHEN MATCHED AND (S._source_sequence > T._source_sequence
              OR (S._source_sequence = T._source_sequence AND S._cdc_timestamp > T._cdc_timestamp))
```

Effects:
* A replayed batch changes nothing, so it's idempotent. This is proven on DuckDB in
  `bigquery/tests/test_merge_idempotency.py`.
* An older batch that arrives late is ignored, so ordering is safe.
* The MERGE also dedups again with `QUALIFY ROW_NUMBER()`, because the loader doesn't trust
  upstream.

**Deletes.** These only apply to existing primary keys. An insert that was never applied
doesn't create a tombstone.

* **Soft (default).** Sets `_is_deleted = TRUE` and keeps the delete's sequence. A late,
  *older* update therefore can't bring the row back; a *newer* re-insert does revive it.
  Consumers filter `WHERE NOT _is_deleted` (see the view in `dashboard_queries.sql`).
* **Hard.** Removes the row. This is correct for in-order delivery. The trade-off: with no
  tombstone, a late older update for that key would be inserted again. Use hard deletes only
  where physical erasure is required (e.g. GDPR), and also purge raw/processed copies under
  the retention policy.
* Final tables always have the `_is_deleted` column, so switching strategy never needs a
  schema change. With hard deletes it's always FALSE.

**Source requirements.** `REPLICA IDENTITY FULL` is set so delete and update images carry
every column. Without it, deletes carry only the PK (still handled: required-column checks are
skipped for deletes), and unchanged TOASTed values can be missing from updates.

## 5. Idempotency (rerun safety) per task

| Task | Why it's safe to rerun |
|---|---|
| check_new_cdc_files | Reuses the persisted input manifest for the same `run_id` |
| run_glue_cdc_job | Same input files; overwrites only `…/run_id=<r>/` prefixes |
| validate_glue_output | Read-only |
| transfer_s3_to_gcs | Skips destination objects with the same MD5; the loader uses the listed files, not wildcards |
| load_gcs_to_bigquery_staging | WRITE_TRUNCATE of the run's staging table, then a count check |
| merge / apply_deletes | Sequence guard means no-ops on replay |
| update_watermark | Monotonic (`max`) |
| publish_audit_metrics | Append-only log, one row per attempt (by design) |

The `run_id` comes from the DAG run's logical date (`cdc_YYYYMMDDTHHMMSSZ`), so clearing and
re-running a DAG run reproduces the same paths. `max_active_runs=1` keeps staging tables and
watermarks single-writer.

## 6. Cross-cloud transfer design

**Chosen: a Glue Python Shell job using `google-cloud-storage`**, authenticated with
**Workload Identity Federation**. The job's AWS role gets a short-lived GCP token, so no
service-account key exists anywhere. The credential configuration (it contains no secret)
is stored in Secrets Manager. If WIF isn't possible, a service-account key in Secrets Manager
is the fallback, rotated every 90 days.

| Option | Pros | Cons |
|---|---|---|
| **Glue Python Shell (chosen)** | Manifest-scoped copy, MD5 checks, writes audit metadata, and runs on the AWS side next to the data | Python 3.9 runtime; one more job to deploy |
| Airflow-worker transfer (`TRANSFER_MODE=airflow_worker`) | No extra job; simplest locally | Data flows through the Airflow workers, which then need sizing and network access to both clouds |
| GCP Storage Transfer Service | Fully managed, parallel | Prefix-based, not manifest-scoped; harder to tie to a run; no per-file audit |
| BigQuery Omni / DTS for S3 | No copy, or managed load | Region limits; no MERGE semantics; Omni needs a supported AWS region |
| ECS/Fargate task | Python 3.11 and custom images | More infrastructure (cluster, task definition, networking) |

Network path: the Glue jobs run in the AWS-managed network with internet egress to
`storage.googleapis.com` over TLS. RDS and DMS sit in private subnets with no NAT; they reach
AWS services through VPC endpoints (S3 gateway, plus Secrets Manager, KMS, Logs and Monitoring
interface endpoints). See [docs/security.md](docs/security.md).

## 7. State and watermarks

*Decision:* watermarks live in **JSON control files in the processed bucket**
(`control/watermarks/<table>.json`).
* They sit next to the data they describe, survive Airflow metadata-DB loss or rebuilds,
  and work the same locally.
* They're readable by Glue, Airflow and humans.
* They move forward only, and only after MERGE and deletes succeed.
* The overlap window (15 minutes by default) guards against objects whose LastModified is
  earlier than an already-listed object. Re-reading those files is free in correctness terms
  because of §4.

The audit table (`cdc_audit.cdc_pipeline_audit`) is the queryable history.

## 8. Failure and recovery model

| Failure | Effect | Recovery |
|---|---|---|
| DMS task stops | Changes queue in the replication slot (WAL retained) | Resume the task; alarm on `OldestReplicationSlotLag` |
| Glue fails | Manifest FAILED or missing; DAG stops at `run_glue_cdc_job`/`validate` | Task retry (exponential backoff), or clear the DAG run (same `run_id`) |
| Too many rejected rows | `validate_glue_output` fails before anything crosses clouds | Inspect `rejected/`, fix the source or contract, rerun |
| Transfer fails part-way | `transfer_result.json` FAILED; copied files are skipped on retry | Retry |
| Staging count mismatch | Load fails before MERGE | Retry (transfer race) or investigate |
| MERGE fails | BigQuery DML is atomic per statement, so the target is unchanged | Retry |
| Failure after MERGE, before watermark | Watermark not advanced; next run re-reads the files | Harmless (idempotent MERGE) |
| GCP outage | Data accumulates in S3; DAG fails and alerts | Runs catch up from the watermark |

No data is lost because raw files are immutable and kept for 90 days. No event is applied
twice in effect, because of the sequence guard.

## 9. Schema ownership

`config/pipeline_config.yaml` is the single contract. Glue's explicit schemas, the BigQuery
DDL (`deploy_bigquery.py`), the staging tables and the SQL are all generated from it.
Terraform owns datasets, IAM and buckets, but **not** table schemas, so two tools never
manage the same object. Schema evolution is described in
[OPERATIONS.md](OPERATIONS.md#schema-evolution).

## 10. Scalability

* **DMS:** scale the instance class, use parallel full-load subtasks, and split large tables
  into separate tasks.
* **Glue:** a single shuffle (PK window); scale workers or G.2X; file sizing via
  `rows_per_file`; one table per Spark job stage. Very large tables can run as separate DAG
  runs (`tables` param).
* **BigQuery:** final tables are partitioned and clustered by PK, which reduces MERGE cost.
  For very large targets, add a partition-range predicate to the MERGE when the partition
  column never changes (left out by default because it is unsafe otherwise).
* **Airflow:** process tables in parallel with dynamic task mapping over `tables` if needed.
  The current DAG runs them sequentially within a run for simplicity.

## 11. Trade-offs vs other approaches

| Approach | When it's better | Why not here |
|---|---|---|
| **Dataflow** (DMS→Kinesis→Pub/Sub→Dataflow→BigQuery) | Seconds-level latency, streaming everywhere | Higher steady cost, cross-cloud streaming egress, more complex exactly-once handling |
| **Pub/Sub + BigQuery CDC upserts** (Storage Write API `_CHANGE_TYPE`) | Near-real-time upserts with no MERGE jobs | Needs a streaming producer; harder to backfill and replay from S3; less control over deletes and ordering |
| **Kafka + Debezium** | Many consumers, event-driven architecture | Kafka clusters to operate; DMS is managed and enough for analytics replication |
| **Fivetran / Airbyte-style SaaS** | Fastest to set up, many connectors | Per-row pricing, less control over validation and audit, data leaves your accounts |

This design is batch micro-CDC: latency is minutes (DMS buffer + schedule). In return you get
an auditable, replayable, low-cost pipeline.
