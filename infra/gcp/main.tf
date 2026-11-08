locals {
  name = "${var.project}-${var.env}"
  labels = {
    project    = var.project
    env        = var.env
    managed_by = "terraform"
    pipeline   = "aws_rds_cdc_to_bigquery"
  }
}

data "google_project" "this" {
  project_id = var.gcp_project_id
}

resource "google_project_service" "apis" {
  for_each = toset([
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "sts.googleapis.com",
    "secretmanager.googleapis.com",
    "logging.googleapis.com",
  ])
  service            = each.value
  disable_on_destroy = false
}
