output "staging_bucket" {
  description = "Set as GCS_STAGING_URI=gs://<value> and infra/aws var gcs_staging_bucket."
  value       = google_storage_bucket.staging.name
}

output "gcs_writer_service_account" {
  value = google_service_account.gcs_writer.email
}

output "bq_loader_service_account" {
  value = google_service_account.bq_loader.email
}

output "workload_identity_provider" {
  description = "Use with `gcloud iam workload-identity-pools create-cred-config` (scripts/bootstrap_gcp.sh)."
  value       = google_iam_workload_identity_pool_provider.aws.name
}

output "datasets" {
  value = { for k, d in google_bigquery_dataset.this : k => d.dataset_id }
}
