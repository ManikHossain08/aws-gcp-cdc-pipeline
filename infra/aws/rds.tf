resource "aws_kms_key" "data" {
  description             = "${local.name} CDC data (RDS, S3, DMS, Glue, Secrets)"
  enable_key_rotation     = true
  deletion_window_in_days = 30
  policy                  = data.aws_iam_policy_document.kms.json
}

# Account root administers the key (IAM policies grant usage); CloudWatch Logs needs an
# explicit grant to encrypt the Glue log groups.
data "aws_iam_policy_document" "kms" {
  statement {
    sid       = "AccountAdmin"
    actions   = ["kms:*"]
    resources = ["*"]
    principals {
      type        = "AWS"
      identifiers = ["arn:${local.partition}:iam::${local.account_id}:root"]
    }
  }
  statement {
    sid       = "CloudWatchLogs"
    actions   = ["kms:Encrypt*", "kms:Decrypt*", "kms:ReEncrypt*", "kms:GenerateDataKey*", "kms:Describe*"]
    resources = ["*"]
    principals {
      type        = "Service"
      identifiers = ["logs.${var.aws_region}.amazonaws.com"]
    }
    condition {
      test     = "ArnLike"
      variable = "kms:EncryptionContext:aws:logs:arn"
      values   = ["arn:${local.partition}:logs:${var.aws_region}:${local.account_id}:*"]
    }
  }
}

resource "aws_kms_alias" "data" {
  name          = "alias/${local.name}-data"
  target_key_id = aws_kms_key.data.key_id
}

resource "aws_db_subnet_group" "this" {
  name       = "${local.name}-rds"
  subnet_ids = aws_subnet.private[*].id
}

resource "aws_db_parameter_group" "cdc" {
  name        = "${local.name}-pg16-logical"
  family      = "postgres16"
  description = "Logical replication for AWS DMS CDC"

  parameter {
    name         = "rds.logical_replication"
    value        = "1"
    apply_method = "pending-reboot"
  }
  parameter {
    name         = "wal_sender_timeout"
    value        = "0"
    apply_method = "immediate"
  }
  parameter {
    name         = "max_replication_slots"
    value        = "10"
    apply_method = "pending-reboot"
  }
  parameter {
    name         = "max_wal_senders"
    value        = "10"
    apply_method = "pending-reboot"
  }
  parameter {
    name         = "rds.force_ssl"
    value        = "1"
    apply_method = "pending-reboot"
  }
}

resource "aws_db_instance" "source" {
  identifier     = "${local.name}-source-pg"
  engine         = "postgres"
  engine_version = var.db_engine_version
  instance_class = var.db_instance_class
  db_name        = var.db_name
  username       = var.db_admin_username

  # RDS generates and rotates the admin password in Secrets Manager — it never appears
  # in Terraform code, variables, or state.
  manage_master_user_password   = true
  master_user_secret_kms_key_id = aws_kms_key.data.arn

  allocated_storage     = var.db_allocated_storage_gb
  max_allocated_storage = var.db_allocated_storage_gb * 4
  storage_type          = "gp3"
  storage_encrypted     = true
  kms_key_id            = aws_kms_key.data.arn

  db_subnet_group_name   = aws_db_subnet_group.this.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  parameter_group_name   = aws_db_parameter_group.cdc.name
  publicly_accessible    = false
  multi_az               = var.db_multi_az

  backup_retention_period         = 7
  deletion_protection             = var.db_deletion_protection
  skip_final_snapshot             = var.env == "dev"
  final_snapshot_identifier       = var.env == "dev" ? null : "${local.name}-source-pg-final"
  performance_insights_enabled    = true
  performance_insights_kms_key_id = aws_kms_key.data.arn
  enabled_cloudwatch_logs_exports = ["postgresql"]
  auto_minor_version_upgrade      = true
  apply_immediately               = var.env != "prod"
}

# Credentials of the least-privilege DMS user (see source_db/init/03_enable_cdc.sql).
# Terraform creates the secret container only; scripts/bootstrap_aws.sh creates the DB user
# and stores {"username","password","host","port"} so no password ever lands in state.
resource "aws_secretsmanager_secret" "dms_source" {
  name                    = "${local.name}/dms/source-postgres"
  description             = "DMS CDC user for ${aws_db_instance.source.identifier}"
  kms_key_id              = aws_kms_key.data.arn
  recovery_window_in_days = var.env == "dev" ? 0 : 30
}

# GCP credential configuration for the S3->GCS transfer: Workload Identity Federation
# config (preferred, contains no secret) or SA key (fallback). Value set by
# scripts/bootstrap_gcp.sh output -> `aws secretsmanager put-secret-value`.
resource "aws_secretsmanager_secret" "gcp_credentials" {
  name                    = "${local.name}/gcp/transfer-credentials"
  description             = "GCP WIF credential config (or SA key) for the GCS writer"
  kms_key_id              = aws_kms_key.data.arn
  recovery_window_in_days = var.env == "dev" ? 0 : 30
}
