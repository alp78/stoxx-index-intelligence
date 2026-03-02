# --------------------------------------------------------------------------
# Artifact Registry (Docker images)
# --------------------------------------------------------------------------
resource "google_artifact_registry_repository" "stoxx" {
  location      = var.region
  repository_id = "stoxx"
  format        = "DOCKER"
}
