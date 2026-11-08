locals {
  raw_arn       = aws_s3_bucket.data["raw"].arn
  processed_arn = aws_s3_bucket.data["processed"].arn
  artifacts_arn = aws_s3_bucket.data["artifacts"].arn
}

data "aws_iam_policy_document" "assume_service" {
  for_each = {
    dms  = "dms.amazonaws.com"
    glue = "glue.amazonaws.com"
  }
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = [each.value]
    }
  }
}

# ------------------------------------------------------------------ DMS account-level roles
# AWS requires these exact names, once per account.
resource "aws_iam_role" "dms_vpc" {
  count              = var.create_dms_service_roles ? 1 : 0
  name               = "dms-vpc-role"
  assume_role_policy = data.aws_iam_policy_document.assume_service["dms"].json
}

resource "aws_iam_role_policy_attachment" "dms_vpc" {
  count      = var.create_dms_service_roles ? 1 : 0
  role       = aws_iam_role.dms_vpc[0].name
  policy_arn = "arn:${local.partition}:iam::aws:policy/service-role/AmazonDMSVPCManagementRole"
}

resource "aws_iam_role" "dms_cloudwatch" {
  count              = var.create_dms_service_roles ? 1 : 0
  name               = "dms-cloudwatch-logs-role"
  assume_role_policy = data.aws_iam_policy_document.assume_service["dms"].json
}

resource "aws_iam_role_policy_attachment" "dms_cloudwatch" {
  count      = var.create_dms_service_roles ? 1 : 0
  role       = aws_iam_role.dms_cloudwatch[0].name
  policy_arn = "arn:${local.partition}:iam::aws:policy/service-role/AmazonDMSCloudWatchLogsRole"
}

# ------------------------------------------------------------------ DMS -> S3 raw (write only)
resource "aws_iam_role" "dms_s3" {
  name               = "${local.name}-dms-s3-target"
  assume_role_policy = data.aws_iam_policy_document.assume_service["dms"].json
}

resource "aws_iam_role_policy" "dms_s3" {
  role = aws_iam_role.dms_s3.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:PutObject", "s3:DeleteObject", "s3:PutObjectTagging"],
      Resource = "${local.raw_arn}/${local.raw_prefix}/*" },
      { Effect = "Allow", Action = ["s3:ListBucket"], Resource = local.raw_arn },
      { Effect = "Allow", Action = ["kms:GenerateDataKey", "kms:Decrypt"], Resource = aws_kms_key.data.arn },
    ]
  })
}

# ------------------------------------------------------------------ DMS -> Secrets Manager
resource "aws_iam_role" "dms_secrets" {
  name = "${local.name}-dms-secrets"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "dms.${var.aws_region}.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "dms_secrets" {
  role = aws_iam_role.dms_secrets.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"],
      Resource = aws_secretsmanager_secret.dms_source.arn },
      { Effect = "Allow", Action = ["kms:Decrypt"], Resource = aws_kms_key.data.arn },
    ]
  })
}

# ------------------------------------------------------------------ Glue transform
resource "aws_iam_role" "glue_transform" {
  name               = "${local.name}-glue-transform"
  assume_role_policy = data.aws_iam_policy_document.assume_service["glue"].json
}

resource "aws_iam_role_policy_attachment" "glue_transform_service" {
  role       = aws_iam_role.glue_transform.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_transform" {
  role = aws_iam_role.glue_transform.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Sid = "ReadRaw", Effect = "Allow", Action = ["s3:GetObject"], Resource = "${local.raw_arn}/${local.raw_prefix}/*" },
      { Sid = "List", Effect = "Allow", Action = ["s3:ListBucket"],
      Resource = [local.raw_arn, local.processed_arn, local.artifacts_arn] },
      { Sid = "WriteProcessed", Effect = "Allow", Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      Resource = [for p in ["validated", "deduplicated", "rejected", "audit"] : "${local.processed_arn}/${p}/*"] },
      { Sid = "ReadRunManifests", Effect = "Allow", Action = ["s3:GetObject"], Resource = "${local.processed_arn}/control/runs/*" },
      { Sid = "Artifacts", Effect = "Allow", Action = ["s3:GetObject", "s3:PutObject"], Resource = "${local.artifacts_arn}/*" },
      { Sid = "Kms", Effect = "Allow", Action = ["kms:GenerateDataKey", "kms:Decrypt", "kms:Encrypt"], Resource = aws_kms_key.data.arn },
    ]
  })
}

# ------------------------------------------------------------------ Glue transfer (S3 -> GCS)
resource "aws_iam_role" "glue_transfer" {
  name               = "${local.name}-glue-transfer"
  assume_role_policy = data.aws_iam_policy_document.assume_service["glue"].json
}

resource "aws_iam_role_policy_attachment" "glue_transfer_service" {
  role       = aws_iam_role.glue_transfer.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_transfer" {
  role = aws_iam_role.glue_transfer.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Curated output + manifests only. Never raw/, rejected/ or validated/.
      { Sid = "ReadCurated", Effect = "Allow", Action = ["s3:GetObject"],
      Resource = ["${local.processed_arn}/deduplicated/*", "${local.processed_arn}/audit/*"] },
      { Sid = "WriteTransferResult", Effect = "Allow", Action = ["s3:PutObject"], Resource = "${local.processed_arn}/audit/*" },
      { Sid = "List", Effect = "Allow", Action = ["s3:ListBucket"], Resource = [local.processed_arn, local.artifacts_arn] },
      { Sid = "Artifacts", Effect = "Allow", Action = ["s3:GetObject"], Resource = "${local.artifacts_arn}/*" },
      { Sid = "GcpCredentials", Effect = "Allow", Action = ["secretsmanager:GetSecretValue"],
      Resource = aws_secretsmanager_secret.gcp_credentials.arn },
      { Sid = "Kms", Effect = "Allow", Action = ["kms:Decrypt", "kms:GenerateDataKey"], Resource = aws_kms_key.data.arn },
    ]
  })
}

# ------------------------------------------------------------------ Airflow (orchestrator)
resource "aws_iam_role" "airflow" {
  count = length(var.airflow_trusted_principal_arns) > 0 ? 1 : 0
  name  = "${local.name}-airflow"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { AWS = var.airflow_trusted_principal_arns }
    }]
  })
}

resource "aws_iam_role_policy" "airflow" {
  count = length(aws_iam_role.airflow)
  role  = aws_iam_role.airflow[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Sid = "RunGlueJobs", Effect = "Allow", Action = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns"],
      Resource = [aws_glue_job.cdc_transform.arn, aws_glue_job.transfer.arn] },
      { Sid = "ListRaw", Effect = "Allow", Action = ["s3:ListBucket"], Resource = [local.raw_arn, local.processed_arn] },
      { Sid = "ControlAndAudit", Effect = "Allow", Action = ["s3:GetObject", "s3:PutObject"],
      Resource = ["${local.processed_arn}/control/*", "${local.processed_arn}/audit/*"] },
      { Sid = "ReadRawForVerification", Effect = "Allow", Action = ["s3:GetObject"], Resource = "${local.raw_arn}/${local.raw_prefix}/*" },
      { Sid = "Kms", Effect = "Allow", Action = ["kms:Decrypt", "kms:GenerateDataKey"], Resource = aws_kms_key.data.arn },
    ]
  })
}
