# --------------------------------------------------------------------------
# Input variables — project, region, database, observability
# --------------------------------------------------------------------------

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "europe-west1"
}

variable "zone" {
  description = "GCP zone for Compute Engine"
  type        = string
  default     = "europe-west1-b"
}

variable "db_password" {
  description = "Cloud SQL admin password"
  type        = string
  sensitive   = true
}

variable "db_tier" {
  description = "Cloud SQL machine type (minimum for SQL Server: db-custom-1-3840)"
  type        = string
  default     = "db-custom-1-3840"
}

variable "dd_api_key" {
  description = "Datadog API key (leave empty to disable agent)"
  type        = string
  sensitive   = true
  default     = ""
}

variable "labels" {
  description = "Common resource labels"
  type        = map(string)
  default = {
    project     = "stoxx"
    managed-by  = "terraform"
  }
}
