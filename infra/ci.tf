# --------------------------------------------------------------------------
# GitHub Actions CI/CD service account
# --------------------------------------------------------------------------

# creates the CI/CD service account used by GitHub Actions to deploy
resource "google_service_account" "ci" {
  account_id   = "stoxx-ci"                      # becomes stoxx-ci@<project>.iam.gserviceaccount.com
  display_name = "STOXX CI/CD (GitHub Actions)"  # human-readable label
}

# grants CI SA permission to push Docker images to Artifact Registry
resource "google_project_iam_member" "ci_registry" {
  project = var.project_id
  role    = "roles/artifactregistry.writer"  # write access to push images (build → push → deploy)
  member  = "serviceAccount:${google_service_account.ci.email}"
}

# grants CI SA permission to update Cloud Run services and jobs (deploy new images)
resource "google_project_iam_member" "ci_run" {
  project = var.project_id
  role    = "roles/run.developer"  # allows gcloud run services/jobs update commands
  member  = "serviceAccount:${google_service_account.ci.email}"
}

# grants CI SA "act as" permission on the pipeline SA
# required because Cloud Run jobs run as stoxx-pipeline, and the deployer must be allowed to assign that identity
resource "google_service_account_iam_member" "ci_act_as_pipeline" {
  service_account_id = google_service_account.pipeline.name              # the target SA that CI is allowed to impersonate
  role               = "roles/iam.serviceAccountUser"                    # "can deploy things that run as this SA"
  member             = "serviceAccount:${google_service_account.ci.email}"  # the principal (CI SA) receiving this permission
}

# grants CI SA "act as" permission on the dashboard SA
# required because Cloud Run dashboard service runs as stoxx-dashboard
resource "google_service_account_iam_member" "ci_act_as_dashboard" {
  service_account_id = google_service_account.dashboard.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.ci.email}"
}
