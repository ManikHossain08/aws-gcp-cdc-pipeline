output "raw_bucket" {
  value = aws_s3_bucket.data["raw"].id
}

output "processed_bucket" {
  value = aws_s3_bucket.data["processed"].id
}

output "artifacts_bucket" {
  value = aws_s3_bucket.data["artifacts"].id
}

output "access_logs_bucket" {
  value = aws_s3_bucket.logs.id
}

output "rds_endpoint" {
  value = aws_db_instance.source.address
}

output "rds_master_secret_arn" {
  description = "RDS-managed admin secret (used once by scripts/bootstrap_aws.sh)."
  value       = aws_db_instance.source.master_user_secret[0].secret_arn
}

output "dms_source_secret_arn" {
  value = aws_secretsmanager_secret.dms_source.arn
}

output "gcp_credentials_secret_arn" {
  value = aws_secretsmanager_secret.gcp_credentials.arn
}

output "dms_replication_task_arn" {
  value = aws_dms_replication_task.cdc.replication_task_arn
}

output "glue_transform_job_name" {
  value = aws_glue_job.cdc_transform.name
}

output "glue_transfer_job_name" {
  value = aws_glue_job.transfer.name
}

output "glue_transfer_role_arn" {
  description = "Input to infra/gcp var aws_transfer_role_arn (Workload Identity Federation)."
  value       = aws_iam_role.glue_transfer.arn
}

output "airflow_role_arn" {
  value = try(aws_iam_role.airflow[0].arn, null)
}

output "alerts_topic_arn" {
  value = aws_sns_topic.alerts.arn
}
