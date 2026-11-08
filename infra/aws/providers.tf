terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70"
    }
  }

  # Remote state per environment:
  #   terraform init -backend-config="bucket=your-tf-state-bucket" \
  #     -backend-config="key=aws-gcp-cdc/aws/<env>.tfstate" -backend-config="region=us-east-1"
  backend "s3" {}
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = local.tags
  }
}
