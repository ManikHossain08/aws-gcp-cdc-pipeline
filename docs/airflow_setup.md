# Airflow setup

The DAG is `airflow/dags/aws_rds_cdc_to_bigquery.py`. Its task logic lives in
`airflow/plugins/cdc_operators.py`: plain Python, unit-tested, and reused by the local runner.
It targets Airflow 2.10 and also imports on Airflow 3 (where SLAs are skipped).

## Local

```bash
make local-airflow
```

This runs `airflow db migrate` with `AIRFLOW_HOME=.airflow_local` (sqlite), creates the DuckDB
tables, simulates batch 1, runs `airflow dags test` for one logical date, simulates batch 2,
runs it for a second date, and then verifies the result.

For the web UI, either use Docker:

```bash
docker compose --env-file .env -f docker/docker-compose.yml up --build
```

or run standalone from the venv:

```bash
AIRFLOW_HOME=$PWD/.airflow_local AIRFLOW__CORE__DAGS_FOLDER=$PWD/airflow/dags AIRFLOW__CORE__PLUGINS_FOLDER=$PWD/airflow/plugins AIRFLOW__CORE__LOAD_EXAMPLES=False PYTHONPATH=$PWD:$PWD/airflow/plugins .venv/bin/airflow standalone
```

## Production deployment

| Platform | How |
|---|---|
| **MWAA** | Upload `airflow/dags/`, `airflow/plugins/` (as plugins.zip) and a `requirements.txt` with the package wheel (`dist/*.whl`) to the MWAA bucket. The execution role assumes `cdc-<env>-airflow` (Terraform output `airflow_role_arn`). GCP access goes through WIF: set `aws_airflow_role_name` in `infra/gcp`. |
| **Cloud Composer** | `gcloud composer environments storage dags import`; install the wheel as a PyPI package. Grant the Composer SA `iam.serviceAccountTokenCreator` on `bq-loader` (`airflow_gcp_principal`). AWS access through an Airflow `aws_default` connection backed by a secret backend. |
| **Astronomer** | `astro dev init`; copy `dags/` and `plugins/`; `airflow_settings.yaml` follows the Astro format. |

Mount or ship `config/pipeline_config.yaml` and set `PIPELINE_CONFIG_PATH` if it isn't next to
the code.

## Configuration

| Setting | Source | Default |
|---|---|---|
| Schedule | env `CDC_DAG_SCHEDULE` | `0 * * * *` |
| Retries | env `CDC_TASK_RETRIES`; exponential backoff from 1 to 20 minutes | 3 |
| SLA (Airflow 2) | env `CDC_SLA_MINUTES` | 90 |
| Delete strategy | DAG param `delete_strategy` → Variable `cdc_delete_strategy` → env | soft |
| Alerts | env `SLACK_WEBHOOK_URL` (https only), `ALERT_EMAILS` (needs SMTP configured) | off |
| Environment | env `PIPELINE_ENV`, `BQ_BACKEND`, `RAW_FORMAT`, bucket URIs, job names | local |

`max_active_runs=1` and `catchup=False`. Backfills go through params (`ingest_date`, `run_id`),
not catchup, so a backfill never races the live schedule.

## Triggering

```bash
airflow dags trigger aws_rds_cdc_to_bigquery -c '{"tables": ["orders"], "ingest_date": "2026-11-04"}'
```

```bash
airflow dags test aws_rds_cdc_to_bigquery 2026-11-05T10:00:00+00:00
```

## Production hardening ideas

* Swap the in-task Glue polling (`_run_glue_job`) for the deferrable `GlueJobOperator` from
  `apache-airflow-providers-amazon`, so long jobs don't hold a worker slot.
* Fan out tables with dynamic task mapping when there are many tables.
* Trigger the DAG off S3 file arrival with the optional EventBridge rule plus an Airflow
  dataset or API call.
