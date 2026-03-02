# --------------------------------------------------------------------------
# Service accounts
# --------------------------------------------------------------------------
resource "google_service_account" "pipeline" {
  account_id   = "stoxx-pipeline"
  display_name = "STOXX Pipeline"
}

resource "google_service_account" "dashboard" {
  account_id   = "stoxx-dashboard"
  display_name = "STOXX Dashboard"
}

resource "google_service_account" "airflow" {
  account_id   = "stoxx-airflow"
  display_name = "STOXX Airflow"
}

# --------------------------------------------------------------------------
# IAM bindings
# --------------------------------------------------------------------------

# Pipeline: access secrets + Cloud SQL
resource "google_project_iam_member" "pipeline_sql" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_secret_manager_secret_iam_member" "pipeline_secret" {
  secret_id = google_secret_manager_secret.db_password.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.pipeline.email}"
}

# Dashboard: access secrets + Cloud SQL
resource "google_project_iam_member" "dashboard_sql" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.dashboard.email}"
}

resource "google_secret_manager_secret_iam_member" "dashboard_secret" {
  secret_id = google_secret_manager_secret.db_password.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.dashboard.email}"
}

# Airflow: invoke Cloud Run Jobs + view logs
resource "google_project_iam_member" "airflow_run_invoker" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "airflow_run_developer" {
  project = var.project_id
  role    = "roles/run.developer"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "airflow_logs" {
  project = var.project_id
  role    = "roles/logging.viewer"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

# --------------------------------------------------------------------------
# Datadog GCP Integration (conditional on dd_api_key)
# --------------------------------------------------------------------------
resource "google_service_account" "datadog" {
  count        = var.dd_api_key != "" ? 1 : 0
  account_id   = "stoxx-datadog"
  display_name = "Datadog GCP Integration"
}

resource "google_project_iam_member" "datadog_monitoring" {
  count   = var.dd_api_key != "" ? 1 : 0
  project = var.project_id
  role    = "roles/monitoring.viewer"
  member  = "serviceAccount:${google_service_account.datadog[0].email}"
}

resource "google_project_iam_member" "datadog_compute" {
  count   = var.dd_api_key != "" ? 1 : 0
  project = var.project_id
  role    = "roles/compute.viewer"
  member  = "serviceAccount:${google_service_account.datadog[0].email}"
}

resource "google_project_iam_member" "datadog_cloudasset" {
  count   = var.dd_api_key != "" ? 1 : 0
  project = var.project_id
  role    = "roles/cloudasset.viewer"
  member  = "serviceAccount:${google_service_account.datadog[0].email}"
}

resource "google_service_account_key" "datadog" {
  count              = var.dd_api_key != "" ? 1 : 0
  service_account_id = google_service_account.datadog[0].name
}
