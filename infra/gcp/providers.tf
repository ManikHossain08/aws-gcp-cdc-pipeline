terraform {
  required_version = ">= 1.6.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.10"
    }
  }

  # terraform init -backend-config="bucket=your-tf-state-bucket" -backend-config="prefix=aws-gcp-cdc/gcp/<env>"
  backend "gcs" {}
}

provider "google" {
  project = var.gcp_project_id
  region  = var.region

  default_labels = local.labels
}
