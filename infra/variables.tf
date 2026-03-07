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
  description = "SQL Server SA password"
  type        = string
  sensitive   = true
}

variable "admin_ip" {
  description = "Admin public IPv4 for Airflow UI access (e.g. 1.2.3.4)"
  type        = string
  default     = ""
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
