# ------------------------------------------------------------------ service accounts
# gcs-writer: used ONLY by the AWS-side transfer job (via WIF) to write GCS staging.
resource "google_service_account" "gcs_writer" {
  account_id   = "${var.project}-${var.env}-gcs-writer"
  display_name = "CDC S3->GCS transfer writer"
}

# bq-loader: used by Airflow to load staging, run MERGE/DELETE and write audit rows.
resource "google_service_account" "bq_loader" {
  account_id   = "${var.project}-${var.env}-bq-loader"
  display_name = "CDC BigQuery loader (Airflow)"
}

resource "google_storage_bucket_iam_member" "writer_create" {
  bucket = google_storage_bucket.staging.name
  role   = "roles/storage.objectCreator"
  member = google_service_account.gcs_writer.member
}

resource "google_storage_bucket_iam_member" "writer_view" {
  bucket = google_storage_bucket.staging.name
  role   = "roles/storage.objectViewer" # MD5 comparison for idempotent re-uploads
  member = google_service_account.gcs_writer.member
}

resource "google_storage_bucket_iam_member" "loader_view" {
  bucket = google_storage_bucket.staging.name
  role   = "roles/storage.objectViewer"
  member = google_service_account.bq_loader.member
}

resource "google_project_iam_member" "loader_job_user" {
  project = var.gcp_project_id
  role    = "roles/bigquery.jobUser"
  member  = google_service_account.bq_loader.member
}

resource "google_bigquery_dataset_iam_member" "loader_editor" {
  for_each   = google_bigquery_dataset.this
  dataset_id = each.value.dataset_id
  role       = "roles/bigquery.dataEditor" # dataset-scoped, not project-wide
  member     = google_service_account.bq_loader.member
}

# ------------------------------------------------------------------ Workload Identity Federation
# AWS roles exchange their AWS credentials for short-lived GCP tokens: no SA keys exist.
resource "google_iam_workload_identity_pool" "aws" {
  workload_identity_pool_id = "${var.project}-${var.env}-aws"
  display_name              = "AWS ${var.env} (CDC pipeline)"
  depends_on                = [google_project_service.apis]
}

resource "google_iam_workload_identity_pool_provider" "aws" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.aws.workload_identity_pool_id
  workload_identity_pool_provider_id = "aws-${var.aws_account_id}"
  display_name                       = "AWS account ${var.aws_account_id}"

  aws {
    account_id = var.aws_account_id
  }

  attribute_mapping = {
    "google.subject"     = "assertion.arn"
    "attribute.aws_role" = "assertion.arn.contains('assumed-role') ? assertion.arn.extract('{account_arn}assumed-role/') + 'assumed-role/' + assertion.arn.extract('assumed-role/{role_name}/') : assertion.arn"
  }

  # Only the pipeline's roles in that account may federate at all.
  attribute_condition = join(" || ", [for r in compact([var.aws_transfer_role_name, var.aws_airflow_role_name]) :
  "attribute.aws_role == 'arn:aws:sts::${var.aws_account_id}:assumed-role/${r}'"])
}

locals {
  pool = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.aws.name}"
}

resource "google_service_account_iam_member" "transfer_wif" {
  service_account_id = google_service_account.gcs_writer.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "${local.pool}/attribute.aws_role/arn:aws:sts::${var.aws_account_id}:assumed-role/${var.aws_transfer_role_name}"
}

resource "google_service_account_iam_member" "airflow_wif" {
  count              = var.aws_airflow_role_name == "" ? 0 : 1
  service_account_id = google_service_account.bq_loader.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "${local.pool}/attribute.aws_role/arn:aws:sts::${var.aws_account_id}:assumed-role/${var.aws_airflow_role_name}"
}

# Airflow on GCP (e.g. Cloud Composer) impersonates the loader instead.
resource "google_service_account_iam_member" "airflow_gcp" {
  count              = var.airflow_gcp_principal == "" ? 0 : 1
  service_account_id = google_service_account.bq_loader.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = var.airflow_gcp_principal
}
