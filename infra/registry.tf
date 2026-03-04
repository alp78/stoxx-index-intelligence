# --------------------------------------------------------------------------
# Artifact Registry (Docker images)
# --------------------------------------------------------------------------
resource "google_artifact_registry_repository" "stoxx" {
  location      = var.region
  repository_id = "stoxx"
  format        = "DOCKER"

  cleanup_policies {
    id     = "keep-latest-5"
    action = "KEEP"

    most_recent_versions {
      keep_count = 5
    }
  }

  cleanup_policies {
    id     = "delete-untagged"
    action = "DELETE"

    condition {
      tag_state = "UNTAGGED"
    }
  }
}
