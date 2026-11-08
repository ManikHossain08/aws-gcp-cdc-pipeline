#!/usr/bin/env bash
# One-time AWS bootstrap after `make deploy-aws` (dms_start_task = false).
#
#   1. Reboots RDS if the logical-replication parameter group is pending.
#   2. Runs source_db/init/*.sql on RDS as the RDS-managed admin (secret from Secrets Manager).
#   3. Creates the DMS user's password and stores {"username","password","host","port"}
#      in the DMS source secret — the password never touches disk or Terraform state.
#   4. Tests the DMS source endpoint connection.
#
# RDS is private: run this from inside the VPC (AWS CloudShell VPC environment, an SSM
# session on a bastion, or a CI runner with VPC access). Requires: aws, jq, psql, openssl.
#
# Usage: ENV=dev scripts/bootstrap_aws.sh
set -euo pipefail

ENV="${ENV:-dev}"
TF_DIR="$(cd "$(dirname "$0")/../infra/aws" && pwd)"
REPO="$(cd "$(dirname "$0")/.." && pwd)"
out() { terraform -chdir="$TF_DIR" output -raw "$1"; }

RDS_HOST="$(out rds_endpoint)"
MASTER_SECRET="$(out rds_master_secret_arn)"
DMS_SECRET="$(out dms_source_secret_arn)"
DMS_TASK="$(out dms_replication_task_arn)"
DB_NAME="${DB_NAME:-appdb}"
INSTANCE_ID="${INSTANCE_ID:-cdc-${ENV}-source-pg}"

echo ">> Checking parameter group status"
STATUS="$(aws rds describe-db-instances --db-instance-identifier "$INSTANCE_ID" \
  --query 'DBInstances[0].DBParameterGroups[0].ParameterApplyStatus' --output text)"
if [[ "$STATUS" == "pending-reboot" ]]; then
  echo ">> Rebooting $INSTANCE_ID to enable rds.logical_replication"
  aws rds reboot-db-instance --db-instance-identifier "$INSTANCE_ID" >/dev/null
  aws rds wait db-instance-available --db-instance-identifier "$INSTANCE_ID"
fi

MASTER_JSON="$(aws secretsmanager get-secret-value --secret-id "$MASTER_SECRET" --query SecretString --output text)"
export PGHOST="$RDS_HOST" PGDATABASE="$DB_NAME" PGSSLMODE=require
export PGUSER="$(jq -r .username <<<"$MASTER_JSON")"
export PGPASSWORD="$(jq -r .password <<<"$MASTER_JSON")"

echo ">> Applying source schema and CDC prerequisites"
for f in "$REPO"/source_db/init/0*.sql; do
  [[ "$ENV" == "prod" && "$f" == *02_seed_data.sql ]] && continue   # no demo data in prod
  psql -v ON_ERROR_STOP=1 -q -f "$f"
done

echo ">> Setting DMS user password and storing it in Secrets Manager"
DMS_PASSWORD="$(openssl rand -base64 32 | tr -d '/+=' | cut -c1-32)"
psql -v ON_ERROR_STOP=1 -q -c "ALTER ROLE dms_cdc_user WITH PASSWORD '${DMS_PASSWORD}'"
SECRET_JSON="$(jq -n --arg u dms_cdc_user --arg p "$DMS_PASSWORD" --arg h "$RDS_HOST" \
  '{username:$u, password:$p, host:$h, port:5432}')"
aws secretsmanager put-secret-value --secret-id "$DMS_SECRET" --secret-string "$SECRET_JSON" >/dev/null
unset DMS_PASSWORD SECRET_JSON PGPASSWORD

echo ">> Testing DMS source endpoint"
SOURCE_EP="$(aws dms describe-replication-tasks --filters "Name=replication-task-arn,Values=$DMS_TASK" \
  --query 'ReplicationTasks[0].SourceEndpointArn' --output text)"
INSTANCE_ARN="$(aws dms describe-replication-tasks --filters "Name=replication-task-arn,Values=$DMS_TASK" \
  --query 'ReplicationTasks[0].ReplicationInstanceArn' --output text)"
aws dms test-connection --replication-instance-arn "$INSTANCE_ARN" --endpoint-arn "$SOURCE_EP" >/dev/null
echo "   connection test started; check: aws dms describe-connections"

echo ">> Done. Next: set dms_start_task = true in infra/aws/envs/${ENV}.tfvars and re-apply."
