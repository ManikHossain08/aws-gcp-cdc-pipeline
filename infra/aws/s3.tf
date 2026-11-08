# Buckets
#   raw        immutable DMS landing zone (versioned, SSE-KMS)        s3://<raw>/cdc/
#   processed  validated / deduplicated / rejected / audit / control  (SSE-KMS)
#   artifacts  Glue scripts, wheels/zips, pipeline_config.yaml       (SSE-KMS)
#   logs       S3 server access logs (SSE-S3: log delivery does not support SSE-KMS)
locals {
  data_buckets = {
    raw       = "${local.name}-raw-${local.account_id}"
    processed = "${local.name}-processed-${local.account_id}"
    artifacts = "${local.name}-artifacts-${local.account_id}"
  }
}

resource "aws_s3_bucket" "logs" {
  bucket        = "${local.name}-access-logs-${local.account_id}"
  force_destroy = var.env == "dev"
}

resource "aws_s3_bucket" "data" {
  for_each      = local.data_buckets
  bucket        = each.value
  force_destroy = var.env == "dev"
}

locals {
  all_buckets = merge({ for k, b in aws_s3_bucket.data : k => b }, { logs = aws_s3_bucket.logs })
}

resource "aws_s3_bucket_public_access_block" "all" {
  for_each                = local.all_buckets
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "all" {
  for_each = local.all_buckets
  bucket   = each.value.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  for_each = aws_s3_bucket.data
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.data.arn
    }
    bucket_key_enabled = true # far fewer KMS requests for many small CDC files
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "logs" {
  bucket = aws_s3_bucket.logs.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "data" {
  for_each = aws_s3_bucket.data
  bucket   = each.value.id
  versioning_configuration {
    status = each.key == "processed" ? "Suspended" : "Enabled"
  }
}

resource "aws_s3_bucket_logging" "data" {
  for_each      = aws_s3_bucket.data
  bucket        = each.value.id
  target_bucket = aws_s3_bucket.logs.id
  target_prefix = "s3/${each.key}/"
}

data "aws_iam_policy_document" "logs_bucket" {
  statement {
    sid       = "S3ServerAccessLogsPolicy"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.logs.arn}/s3/*"]
    principals {
      type        = "Service"
      identifiers = ["logging.s3.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.logs.arn, "${aws_s3_bucket.logs.arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "logs" {
  bucket     = aws_s3_bucket.logs.id
  policy     = data.aws_iam_policy_document.logs_bucket.json
  depends_on = [aws_s3_bucket_public_access_block.all]
}

data "aws_iam_policy_document" "tls_only" {
  for_each = aws_s3_bucket.data
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [each.value.arn, "${each.value.arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "data" {
  for_each   = aws_s3_bucket.data
  bucket     = each.value.id
  policy     = data.aws_iam_policy_document.tls_only[each.key].json
  depends_on = [aws_s3_bucket_public_access_block.all]
}

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.data["raw"].id
  rule {
    id     = "raw-retention"
    status = "Enabled"
    filter {
      prefix = "${local.raw_prefix}/"
    }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    expiration {
      days = var.raw_retention_days
    }
    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "processed" {
  bucket = aws_s3_bucket.data["processed"].id
  dynamic "rule" {
    for_each = toset(["validated", "deduplicated", "rejected"])
    content {
      id     = "${rule.value}-retention"
      status = "Enabled"
      filter {
        prefix = "${rule.value}/"
      }
      expiration {
        days = var.processed_retention_days
      }
    }
  }
  # audit/ and control/ are kept (small; needed for lineage and watermarks).
  rule {
    id     = "abort-multipart"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "logs" {
  bucket = aws_s3_bucket.logs.id
  rule {
    id     = "logs-retention"
    status = "Enabled"
    filter {}
    expiration {
      days = var.access_log_retention_days
    }
  }
}

# Optional: raw ObjectCreated -> EventBridge (file-arrival notifications).
resource "aws_s3_bucket_notification" "raw" {
  count       = var.enable_file_arrival_events ? 1 : 0
  bucket      = aws_s3_bucket.data["raw"].id
  eventbridge = true
}

# ------------------------------------------------------------------ artifacts
resource "aws_s3_object" "pipeline_config" {
  bucket = aws_s3_bucket.data["artifacts"].id
  key    = "config/pipeline_config.yaml"
  source = "${local.repo_root}/config/pipeline_config.yaml"
  etag   = filemd5("${local.repo_root}/config/pipeline_config.yaml")
}

resource "aws_s3_object" "glue_transform_script" {
  bucket = aws_s3_bucket.data["artifacts"].id
  key    = "glue/cdc_transform_job.py"
  source = "${local.repo_root}/glue/cdc_transform_job.py"
  etag   = filemd5("${local.repo_root}/glue/cdc_transform_job.py")
}

resource "aws_s3_object" "transfer_script" {
  bucket = aws_s3_bucket.data["artifacts"].id
  key    = "glue/s3_to_gcs_transfer.py"
  source = "${local.repo_root}/transfer/s3_to_gcs_transfer.py"
  etag   = filemd5("${local.repo_root}/transfer/s3_to_gcs_transfer.py")
}

# Built by `make build` (cdc_common + glue + transfer packages).
resource "aws_s3_object" "pipeline_libs" {
  bucket = aws_s3_bucket.data["artifacts"].id
  key    = "glue/pipeline_libs.zip"
  source = "${local.repo_root}/dist/pipeline_libs.zip"
  etag   = filemd5("${local.repo_root}/dist/pipeline_libs.zip")
}

resource "aws_s3_object" "pipeline_wheel" {
  bucket = aws_s3_bucket.data["artifacts"].id
  key    = "glue/${local.wheel_name}"
  source = "${local.repo_root}/dist/${local.wheel_name}"
  etag   = filemd5("${local.repo_root}/dist/${local.wheel_name}")
}

locals {
  wheel_name = "aws_gcp_cdc_pipeline-${var.package_version}-py3-none-any.whl"
}
