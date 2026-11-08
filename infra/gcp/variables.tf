variable "env" {
  type = string
  validation {
    condition     = contains(["dev", "test", "prod"], var.env)
    error_message = "env must be dev, test or prod."
  }
}

variable "project" {
  type    = string
  default = "cdc"
}

variable "gcp_project_id" {
  description = "Target GCP project (placeholder: your-gcp-project-id)."
  type        = string
}

variable "region" {
  description = "Region of the GCS staging bucket. Keep it close to the AWS region (egress latency)."
  type        = string
  default     = "us-east4"
}

variable "bq_location" {
  description = "BigQuery location — data residency decision; staging bucket must be compatible."
  type        = string
  default     = "us-east4"
}

variable "staging_dataset" {
  type    = string
  default = "cdc_staging"
}

variable "final_dataset" {
  type    = string
  default = "analytics"
}

variable "audit_dataset" {
  type    = string
  default = "cdc_audit"
}

variable "staging_retention_days" {
  description = "GCS staging objects are deleted after N days (BigQuery holds the data)."
  type        = number
  default     = 7
}

# ------------------------------------------------------------------ Workload Identity Federation
variable "aws_account_id" {
  description = "AWS account whose roles may impersonate the GCS writer (placeholder: your-aws-account-id)."
  type        = string
}

variable "aws_transfer_role_name" {
  description = "IAM role name of the Glue transfer job (infra/aws output glue_transfer_role_arn, name part)."
  type        = string
  default     = "cdc-dev-glue-transfer"
}

variable "aws_airflow_role_name" {
  description = "Optional IAM role name of Airflow on AWS (MWAA) allowed to run BigQuery jobs."
  type        = string
  default     = ""
}

variable "airflow_gcp_principal" {
  description = "Optional GCP principal running Airflow on GCP (e.g. serviceAccount:composer@...)."
  type        = string
  default     = ""
}
