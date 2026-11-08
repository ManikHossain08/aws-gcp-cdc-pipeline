variable "env" {
  description = "Environment: dev | test | prod."
  type        = string
  validation {
    condition     = contains(["dev", "test", "prod"], var.env)
    error_message = "env must be dev, test or prod."
  }
}

variable "project" {
  description = "Short name used as a prefix for every resource."
  type        = string
  default     = "cdc"
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

# ------------------------------------------------------------------ network
variable "vpc_cidr" {
  type    = string
  default = "10.40.0.0/16"
}

variable "private_subnet_cidrs" {
  description = "At least two private subnets in different AZs (RDS + DMS)."
  type        = list(string)
  default     = ["10.40.1.0/24", "10.40.2.0/24"]
}

# ------------------------------------------------------------------ RDS
variable "db_name" {
  type    = string
  default = "appdb"
}

variable "db_admin_username" {
  type    = string
  default = "cdc_admin"
}

variable "db_instance_class" {
  type    = string
  default = "db.t4g.medium"
}

variable "db_engine_version" {
  type    = string
  default = "16.4"
}

variable "db_allocated_storage_gb" {
  type    = number
  default = 50
}

variable "db_multi_az" {
  type    = bool
  default = false
}

variable "db_deletion_protection" {
  type    = bool
  default = true
}

# ------------------------------------------------------------------ DMS
variable "dms_instance_class" {
  type    = string
  default = "dms.t3.medium"
}

variable "dms_allocated_storage_gb" {
  type    = number
  default = 50
}

variable "dms_engine_version" {
  type    = string
  default = "3.5.4"
}

variable "dms_multi_az" {
  type    = bool
  default = false
}

variable "dms_start_task" {
  description = "Start the replication task on apply (only after scripts/bootstrap_aws.sh)."
  type        = bool
  default     = false
}

variable "create_dms_service_roles" {
  description = "Create account-level dms-vpc-role and dms-cloudwatch-logs-role (once per account)."
  type        = bool
  default     = false
}

variable "dms_cdc_max_batch_interval_seconds" {
  type    = number
  default = 60
}

# ------------------------------------------------------------------ Glue
variable "glue_worker_type" {
  type    = string
  default = "G.1X"
}

variable "glue_number_of_workers" {
  type    = number
  default = 2
}

variable "gcp_project_id" {
  description = "GCP project hosting BigQuery/GCS (passed to Glue as configuration)."
  type        = string
  default     = "your-gcp-project-id"
}

variable "gcs_staging_bucket" {
  description = "GCS staging bucket name (output of infra/gcp)."
  type        = string
  default     = "your-staging-bucket-name"
}

# ------------------------------------------------------------------ access / retention
variable "airflow_trusted_principal_arns" {
  description = "IAM principals allowed to assume the Airflow role (e.g. the MWAA execution role)."
  type        = list(string)
  default     = []
}

variable "raw_retention_days" {
  type    = number
  default = 90
}

variable "processed_retention_days" {
  type    = number
  default = 30
}

variable "access_log_retention_days" {
  type    = number
  default = 365
}

variable "enable_file_arrival_events" {
  description = "Send raw-bucket ObjectCreated events to EventBridge (logged; usable by event-driven triggers)."
  type        = bool
  default     = false
}

variable "alert_email" {
  type    = string
  default = ""
}

variable "replication_slot_lag_alarm_bytes" {
  type    = number
  default = 5368709120 # 5 GiB
}

variable "package_version" {
  description = "Version in pyproject.toml; selects dist/aws_gcp_cdc_pipeline-<v>-py3-none-any.whl."
  type        = string
  default     = "1.0.0"
}
