resource "google_storage_bucket" "staging" {
  name                        = "${local.name}-cdc-staging-${var.gcp_project_id}"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = var.env == "dev"

  versioning {
    enabled = false # staging is reproducible from S3 processed zone
  }

  lifecycle_rule {
    condition {
      age = var.staging_retention_days
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.apis]
}
