# --------------------------------------------------------------------------
# Service accounts
# --------------------------------------------------------------------------

# creates the pipeline service account (runtime identity for Cloud Run pipeline + setup jobs)
resource "google_service_account" "pipeline" {
  account_id   = "stoxx-pipeline"  # short name, becomes stoxx-pipeline@<project>.iam.gserviceaccount.com
  display_name = "STOXX Pipeline"  # human-readable label shown in GCP Console
}

# creates the dashboard service account (runtime identity for Cloud Run dashboard service)
resource "google_service_account" "dashboard" {
  account_id   = "stoxx-dashboard"
  display_name = "STOXX Dashboard"
}

# creates the Airflow service account (runtime identity for the Airflow GCE VM)
resource "google_service_account" "airflow" {
  account_id   = "stoxx-airflow"
  display_name = "STOXX Airflow"
}

# --------------------------------------------------------------------------
# IAM bindings
# --------------------------------------------------------------------------

# grants pipeline SA access to the DB password secret in Secret Manager
resource "google_secret_manager_secret_iam_member" "pipeline_secret" {
  secret_id = google_secret_manager_secret.db_password.id                # the specific secret this binding applies to (resource-level, not project-level)
  role      = "roles/secretmanager.secretAccessor"                       # allows reading the secret value
  member    = "serviceAccount:${google_service_account.pipeline.email}"  # the principal (who) receiving this permission
}

# grants dashboard SA access to the DB password secret in Secret Manager
resource "google_secret_manager_secret_iam_member" "dashboard_secret" {
  secret_id = google_secret_manager_secret.db_password.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.dashboard.email}"
}

# grants Airflow SA permission to invoke Cloud Run services and jobs
resource "google_project_iam_member" "airflow_run_invoker" {
  project = var.project_id  # project-level binding (applies to all Cloud Run resources)
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

# grants Airflow SA permission to manage Cloud Run (update, describe jobs)
resource "google_project_iam_member" "airflow_run_developer" {
  project = var.project_id
  role    = "roles/run.developer"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

# grants Airflow SA permission to view Cloud Run execution logs
resource "google_project_iam_member" "airflow_logs" {
  project = var.project_id
  role    = "roles/logging.viewer"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

# --------------------------------------------------------------------------
# Datadog GCP Integration (conditional on dd_api_key)
# --------------------------------------------------------------------------

# creates the Datadog integration service account (only if dd_api_key is provided)
resource "google_service_account" "datadog" {
  count        = var.dd_api_key != "" ? 1 : 0  # conditional: 0 instances if no API key, 1 if provided
  account_id   = "stoxx-datadog"
  display_name = "Datadog GCP Integration"
}

# grants Datadog SA read access to Cloud Monitoring metrics
resource "google_project_iam_member" "datadog_monitoring" {
  count   = var.dd_api_key != "" ? 1 : 0
  project = var.project_id
  role    = "roles/monitoring.viewer"
  member  = "serviceAccount:${google_service_account.datadog[0].email}"  # references the conditional resource using [0] index
}

# grants Datadog SA read access to Compute Engine metadata (VM inventory)
resource "google_project_iam_member" "datadog_compute" {
  count   = var.dd_api_key != "" ? 1 : 0
  project = var.project_id
  role    = "roles/compute.viewer"
  member  = "serviceAccount:${google_service_account.datadog[0].email}"
}

# grants Datadog SA read access to Cloud Asset inventory
resource "google_project_iam_member" "datadog_cloudasset" {
  count   = var.dd_api_key != "" ? 1 : 0
  project = var.project_id
  role    = "roles/cloudasset.viewer"
  member  = "serviceAccount:${google_service_account.datadog[0].email}"
}

# grants pipeline SA access to the Datadog API key secret
resource "google_secret_manager_secret_iam_member" "pipeline_dd_secret" {
  secret_id = google_secret_manager_secret.dd_api_key.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.pipeline.email}"
}
