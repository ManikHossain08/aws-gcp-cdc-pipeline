# Setup and deployment

## 0. Local environment

```bash
make venv && make install && make test && make local-test
```

## 1. Decide placeholders

| Placeholder | Where |
|---|---|
| `your-aws-account-id` | `infra/gcp/envs/<env>.tfvars` (`aws_account_id`) |
| `your-gcp-project-id` | `infra/gcp` + `infra/aws` tfvars, `.env` (`GCP_PROJECT_ID`) |
| `your-tf-state-bucket` | `envs/<env>.backend.hcl` for both stacks |
| BigQuery location / GCS region | `bq_location`, `region` (pick for data residency, close to the AWS region) |

## 2. GCP (first: AWS needs the staging bucket name, GCP needs the AWS role name)

```bash
cp infra/gcp/envs/dev.tfvars.example infra/gcp/envs/dev.tfvars
```

```bash
cp infra/gcp/envs/dev.backend.hcl.example infra/gcp/envs/dev.backend.hcl
```

```bash
make deploy-gcp ENV=dev
```

This creates the GCS staging bucket, the datasets `cdc_staging`, `analytics` and `cdc_audit`,
two service accounts (`gcs-writer`, `bq-loader`), the Workload Identity pool and AWS provider,
the least-privilege IAM bindings and the `v_pipeline_health` view. It then runs
`deploy_bigquery.py --apply` to create the staging, final and audit tables.

The AWS transfer role name is deterministic (`<project>-<env>-glue-transfer`), so GCP can
trust it before it exists.

## 3. AWS

```bash
cp infra/aws/envs/dev.tfvars.example infra/aws/envs/dev.tfvars      # set gcs_staging_bucket from step 2
```

```bash
cp infra/aws/envs/dev.backend.hcl.example infra/aws/envs/dev.backend.hcl
```

```bash
make deploy-aws ENV=dev
```

`make build` runs first and produces `dist/pipeline_libs.zip` (for Glue Spark) and the wheel
(for Glue Python Shell). Terraform uploads them together with the Glue scripts and
`pipeline_config.yaml`.

Keep `dms_start_task = false` on the first apply.

## 4. Bootstrap

From a machine inside the VPC (CloudShell VPC environment, an SSM bastion, or a VPC CI runner):

```bash
ENV=dev scripts/bootstrap_aws.sh
```

This reboots RDS for logical replication if needed, applies `source_db/init/*.sql` (schema,
seed data in non-prod, DMS user, replica identity, publication, slot), generates the DMS
user's password into Secrets Manager, and tests the endpoint.

```bash
ENV=dev GCP_PROJECT_ID=your-gcp-project-id scripts/bootstrap_gcp.sh
```

This writes the WIF credential configuration for the transfer role into Secrets Manager.

## 5. Start CDC

Set `dms_start_task = true`, then run `make deploy-aws ENV=dev`. The DMS full load runs first,
then ongoing CDC. Files appear under `s3://<raw>/cdc/public/<table>/`.

## 6. Airflow

See [airflow_setup.md](airflow_setup.md). Cloud-mode environment:

```text
PIPELINE_ENV=cloud BQ_BACKEND=bigquery RAW_FORMAT=parquet TRANSFER_MODE=glue_python_shell
RAW_BUCKET_URI=s3://<raw_bucket> PROCESSED_BUCKET_URI=s3://<processed_bucket>
GCS_STAGING_URI=gs://<staging_bucket> GCP_PROJECT_ID=... BQ_LOCATION=...
GLUE_TRANSFORM_JOB_NAME=<glue_transform_job_name> TRANSFER_GLUE_JOB_NAME=<glue_transfer_job_name>
```

## 7. Verify

```bash
airflow dags trigger aws_rds_cdc_to_bigquery
```

```bash
PIPELINE_ENV=cloud BQ_BACKEND=bigquery .venv/bin/python scripts/verify_bigquery_results.py
```

In cloud mode the verifier checks for duplicate PKs, missing CDC metadata and audit rows. The
reference-model replay is local-only, because it reads raw JSONL.

## Teardown (dev)

```bash
terraform -chdir=infra/aws destroy -var-file=envs/dev.tfvars
```

```bash
terraform -chdir=infra/gcp destroy -var-file=envs/dev.tfvars
```

Before destroying, drop the replication slot on RDS (or delete the instance) so WAL stops being
retained.
