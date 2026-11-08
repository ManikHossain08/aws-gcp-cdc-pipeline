resource "aws_glue_catalog_database" "cdc" {
  name        = replace("${local.name}_cdc", "-", "_")
  description = "Catalog for raw and processed CDC zones (query rejected/audit with Athena)"
}

resource "aws_cloudwatch_log_group" "glue" {
  for_each          = toset(["cdc-transform", "s3-to-gcs-transfer"])
  name              = "/aws-glue/jobs/${local.name}-${each.key}"
  retention_in_days = 30
  kms_key_id        = aws_kms_key.data.arn
}

resource "aws_glue_security_configuration" "this" {
  name = "${local.name}-glue"
  encryption_configuration {
    cloudwatch_encryption {
      cloudwatch_encryption_mode = "SSE-KMS"
      kms_key_arn                = aws_kms_key.data.arn
    }
    job_bookmarks_encryption {
      job_bookmarks_encryption_mode = "CSE-KMS"
      kms_key_arn                   = aws_kms_key.data.arn
    }
    s3_encryption {
      s3_encryption_mode = "SSE-KMS"
      kms_key_arn        = aws_kms_key.data.arn
    }
  }
}

locals {
  artifacts = aws_s3_bucket.data["artifacts"].id
  # Values injected into ${VAR} placeholders of pipeline_config.yaml inside Glue.
  glue_config_env = jsonencode({
    PIPELINE_ENV              = "cloud"
    RAW_FORMAT                = "parquet"
    RAW_BUCKET_URI            = "s3://${aws_s3_bucket.data["raw"].id}"
    PROCESSED_BUCKET_URI      = "s3://${aws_s3_bucket.data["processed"].id}"
    GCS_STAGING_URI           = "gs://${var.gcs_staging_bucket}"
    GCP_PROJECT_ID            = var.gcp_project_id
    BQ_BACKEND                = "bigquery"
    AWS_REGION                = var.aws_region
    GCP_CREDENTIALS_SECRET_ID = aws_secretsmanager_secret.gcp_credentials.arn
  })

  common_glue_args = {
    "--config_s3_uri"                    = "s3://${local.artifacts}/${aws_s3_object.pipeline_config.key}"
    "--config_env_json"                  = local.glue_config_env
    "--extra-py-files"                   = "s3://${local.artifacts}/${aws_s3_object.pipeline_libs.key}"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--job-bookmark-option"              = "job-bookmark-disable" # inputs come from run manifests
    "--TempDir"                          = "s3://${local.artifacts}/glue-temp/"
  }
}

resource "aws_glue_job" "cdc_transform" {
  name              = "${local.name}-cdc-transform"
  description       = "Validate, deduplicate and split DMS CDC files (PySpark)"
  role_arn          = aws_iam_role.glue_transform.arn
  glue_version      = "5.0" # Spark 3.5, Python 3.11
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = 60
  max_retries       = 0 # Airflow owns retries (task-level, exponential backoff)

  security_configuration = aws_glue_security_configuration.this.name

  execution_property {
    max_concurrent_runs = 1
  }

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${local.artifacts}/${aws_s3_object.glue_transform_script.key}"
  }

  default_arguments = merge(local.common_glue_args, {
    "--job-language"                 = "python"
    "--continuous-log-logGroup"      = aws_cloudwatch_log_group.glue["cdc-transform"].name
    "--enable-observability-metrics" = "true"
    "--additional-python-modules"    = "PyYAML==6.0.2"
    "--rows_per_file"                = "500000"
  })
}

# Preferred cross-cloud transfer: Glue Python Shell job with google-cloud-storage.
# Note: Python Shell runs Python 3.9; the transfer package avoids 3.10+ only syntax at
# runtime (from __future__ annotations). Use ECS/Fargate if you need 3.11 there too.
resource "aws_glue_job" "transfer" {
  name         = "${local.name}-s3-to-gcs-transfer"
  description  = "Copy manifest-listed curated files to GCS with MD5 verification"
  role_arn     = aws_iam_role.glue_transfer.arn
  glue_version = "3.0"
  max_capacity = 1
  timeout      = 60
  max_retries  = 0

  security_configuration = aws_glue_security_configuration.this.name

  execution_property {
    max_concurrent_runs = 1
  }

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "s3://${local.artifacts}/${aws_s3_object.transfer_script.key}"
  }

  default_arguments = merge(local.common_glue_args, {
    # Python Shell installs packages from wheels (not zips).
    "--extra-py-files"            = "s3://${local.artifacts}/${aws_s3_object.pipeline_wheel.key}"
    "--continuous-log-logGroup"   = aws_cloudwatch_log_group.glue["s3-to-gcs-transfer"].name
    "--additional-python-modules" = "google-cloud-storage==2.18.2,google-auth==2.35.0,PyYAML==6.0.2"
  })
}
