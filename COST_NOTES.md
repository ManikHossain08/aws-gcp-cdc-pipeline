# Cost notes

The figures below are rough list prices in us-east-1 / us-east4 at the time of writing. They
show the order of magnitude only; check current pricing pages before deciding anything.

| Component | Driver | Order of magnitude | Levers |
|---|---|---|---|
| **AWS DMS** | Replication instance hours (always on) + storage | `dms.t3.medium` ≈ $0.07–0.08/h ≈ $55/month single-AZ; Multi-AZ doubles it | Right-size (watch CPU/memory and `CDCLatency*`); single-AZ in dev; DMS Serverless for spiky loads |
| **RDS** | Instance + storage + WAL retention | Depends on the source; WAL piles up when DMS lags | Heartbeat on, alarm on slot lag, `REPLICA IDENTITY FULL` only where needed (bigger WAL) |
| **S3** | Storage + PUT/GET requests + KMS requests | Small for storage; requests dominate with many tiny CDC files | `CdcMinFileSize`/`CdcMaxBatchInterval` → fewer, bigger files; S3 **Bucket Keys** (enabled) cut KMS calls ~99%; lifecycle: raw → IA at 30d, expire at 90d; processed zones expire at 30d |
| **Glue (Spark)** | DPU-hours ($0.44/DPU-h), 1-minute minimum per run | 2 × G.1X for 2 min hourly ≈ 2 DPU × 2/60 h × 24 × 30 ≈ $21/month | Skip runs with no data (the DAG does); batch less often; Flex execution for non-urgent backfills; auto-scaling |
| **Glue Python Shell** | 0.0625–1 DPU per run | Cents per day | Use 0.0625 DPU for small batches |
| **Cross-cloud egress** | AWS data transfer out to the internet (~$0.09/GB first 10 TB) | Usually the **largest variable cost** at volume | Send only deduplicated, Snappy-compressed Parquet (never raw, validated or rejected); co-locate regions (us-east-1 ↔ us-east4); for large volumes look at AWS Direct Connect + Partner Interconnect |
| **GCS staging** | Storage + class A ops | Negligible with a 7-day lifecycle | Keep lifecycle short; Standard class (short-lived objects) |
| **BigQuery storage** | Active / long-term logical storage | Final tables ≈ size of the source tables | Partition expiration on history-like tables; long-term pricing applies automatically after 90 days unmodified |
| **BigQuery queries** | Bytes scanned (on-demand ~$6.25/TiB) or slot reservations | MERGE scans the target table's PK + changed columns | Clustering by PK (done); partitioning; optional partition-range predicate in MERGE when the partition column is immutable; reservations / Editions for predictable cost; avoid `SELECT *` (generated SQL never uses it) |
| **Airflow** | MWAA / Composer environment hours | MWAA small ≈ $0.49/h; Composer 2 small similar | Share an existing environment; this DAG is light |

## Ways to reduce cost

1. **Batch size vs latency:** an hourly schedule gives about 24 Glue runs a day. Running every
   15 minutes costs about 4× as much in Glue minimum charges. The DAG short-circuits when no
   files arrived.
2. **Compression and file sizing:** DMS GZIP Parquet with bigger CDC files, and Snappy Parquet
   out of Glue sized by `rows_per_file`. This means fewer requests and less egress.
3. **Deduplicate before egress:** a key updated 100 times in a batch crosses the cloud boundary
   once.
4. **Partitioning and clustering:** final tables are partitioned on a business date and
   clustered by PK, so MERGE and consumer queries prune.
5. **Right-size DMS and Glue** from CloudWatch metrics after two weeks of real traffic.
6. **Retention:** raw 90d, processed 30d, GCS staging 7d, access logs 365d. Tune these to your
   replay and compliance needs.
7. **Avoid repeated ad-hoc scans:** point dashboards at the small audit table or view, not the
   replica tables.
