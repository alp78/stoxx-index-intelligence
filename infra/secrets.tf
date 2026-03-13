# --------------------------------------------------------------------------
# Secret Manager
# --------------------------------------------------------------------------

# creates the Secret Manager secret container for the DB password
resource "google_secret_manager_secret" "db_password" {
  secret_id = "stoxx-db-password"  # secret name in GCP

  # how the secret is replicated across regions
  replication {
    # Google manages replication automatically (multi-region)
    auto {}
  }
}

# stores the actual password value as a version inside the secret
resource "google_secret_manager_secret_version" "db_password" {
  secret      = google_secret_manager_secret.db_password.id  # reference to the parent secret container
  secret_data = var.db_password                              # the password value from tfvars (pushed to Secret Manager via GCP API)
}

# creates the Secret Manager secret container for the Datadog API key
resource "google_secret_manager_secret" "dd_api_key" {
  secret_id = "stoxx-dd-api-key"  # secret name in GCP

  # how the secret is replicated across regions
  replication {
    auto {}
  }
}

# stores the Datadog API key value (only created if dd_api_key is provided)
resource "google_secret_manager_secret_version" "dd_api_key" {
  count       = var.dd_api_key != "" ? 1 : 0  # conditional creation: 0 if empty, 1 if provided
  secret      = google_secret_manager_secret.dd_api_key.id
  secret_data = var.dd_api_key
}
