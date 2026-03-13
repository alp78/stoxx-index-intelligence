# --------------------------------------------------------------------------
# Terraform provider config — GCP (europe-west1)
# --------------------------------------------------------------------------

# top-level settings: required version, providers, and state backend
terraform {
  required_version = ">= 1.5"  # minimum Terraform CLI version allowed

  # declares which provider plugins to download
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }

  # where Terraform stores the .tfstate file (GCS bucket)
  backend "gcs" {
    bucket = "stoxx-tf-state"   # GCS bucket name holding the remote state
    prefix = "terraform/state"  # folder path inside the bucket
  }
}

# configures the Google provider with project and region defaults
provider "google" {
  project = var.project_id  # GCP project all resources belong to
  region  = var.region      # default region for regional resources
}
