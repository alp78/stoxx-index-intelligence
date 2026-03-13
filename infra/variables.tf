# --------------------------------------------------------------------------
# Input variables — project, region, database, observability
# --------------------------------------------------------------------------

# GCP project ID (no default, must be provided in tfvars)
variable "project_id" {
  description = "GCP project ID"
  type        = string
}

# GCP region for regional resources (Cloud Run, Artifact Registry, subnet)
variable "region" {
  description = "GCP region"
  type        = string
  default     = "europe-west1"  # used when not overridden in tfvars
}

# GCP zone for zonal resources (Compute Engine VMs)
variable "zone" {
  description = "GCP zone for Compute Engine"
  type        = string
  default     = "europe-west1-b"
}

# SQL Server SA password, pushed to Secret Manager
variable "db_password" {
  description = "SQL Server SA password"
  type        = string
  sensitive   = true  # redacts value from plan output and logs
}

# admin public IP for direct Airflow UI access (bypasses IAP)
variable "admin_ip" {
  description = "Admin public IPv4 for Airflow UI access (e.g. 1.2.3.4)"
  type        = string
  default     = ""
}

# Datadog API key, empty string disables the agent
variable "dd_api_key" {
  description = "Datadog API key (leave empty to disable agent)"
  type        = string
  sensitive   = true
  default     = ""
}

# common labels applied to resources for cost tracking and filtering
variable "labels" {
  description = "Common resource labels"
  type        = map(string)
  default = {
    project    = "stoxx"
    managed-by = "terraform"
  }
}
