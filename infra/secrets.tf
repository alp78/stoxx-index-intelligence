# --------------------------------------------------------------------------
# Secret Manager
# --------------------------------------------------------------------------
resource "google_secret_manager_secret" "db_password" {
  secret_id = "stoxx-db-password"

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "db_password" {
  secret      = google_secret_manager_secret.db_password.id
  secret_data = var.db_password
}

resource "google_secret_manager_secret" "dd_api_key" {
  secret_id = "stoxx-dd-api-key"

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "dd_api_key" {
  count       = var.dd_api_key != "" ? 1 : 0
  secret      = google_secret_manager_secret.dd_api_key.id
  secret_data = var.dd_api_key
}
