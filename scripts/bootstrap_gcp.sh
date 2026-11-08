#!/usr/bin/env bash
# GCP bootstrap after `terraform -chdir=infra/gcp apply`:
#   1. Creates BigQuery tables from config/pipeline_config.yaml (deploy_bigquery.py).
#   2. Generates the Workload Identity Federation *credential configuration* for the AWS
#      Glue transfer role (contains no secret) and stores it in AWS Secrets Manager.
#
# Requires: gcloud (authenticated as a project admin), aws, terraform, the project venv.
# Usage: ENV=dev GCP_PROJECT_ID=your-gcp-project-id scripts/bootstrap_gcp.sh
set -euo pipefail

ENV="${ENV:-dev}"
: "${GCP_PROJECT_ID:?set GCP_PROJECT_ID}"
REPO="$(cd "$(dirname "$0")/.." && pwd)"
gout() { terraform -chdir="$REPO/infra/gcp" output -raw "$1"; }
aout() { terraform -chdir="$REPO/infra/aws" output -raw "$1"; }

echo ">> Creating BigQuery tables"
PIPELINE_ENV=cloud BQ_BACKEND=bigquery GCP_PROJECT_ID="$GCP_PROJECT_ID" \
  "$REPO/.venv/bin/python" "$REPO/bigquery/scripts/deploy_bigquery.py" --apply

echo ">> Generating WIF credential configuration for the AWS transfer role"
PROVIDER="$(gout workload_identity_provider)"
WRITER_SA="$(gout gcs_writer_service_account)"
CRED_FILE="$(mktemp)"
trap 'rm -f "$CRED_FILE"' EXIT
gcloud iam workload-identity-pools create-cred-config "$PROVIDER" \
  --service-account="$WRITER_SA" --aws --enable-imdsv2 --output-file="$CRED_FILE"

echo ">> Storing it in AWS Secrets Manager (no key material involved)"
aws secretsmanager put-secret-value --secret-id "$(aout gcp_credentials_secret_arn)" \
  --secret-string "file://$CRED_FILE" >/dev/null

echo ">> Done. Staging bucket: gs://$(gout staging_bucket)"
