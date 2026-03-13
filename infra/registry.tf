# --------------------------------------------------------------------------
# Artifact Registry (Docker images)
# --------------------------------------------------------------------------

# creates the Docker image registry for pipeline and dashboard images
resource "google_artifact_registry_repository" "stoxx" {
  location      = var.region   # region where images are stored
  repository_id = "stoxx"      # registry name in GCP
  format        = "DOCKER"     # DOCKER for container images (also supports MAVEN, NPM, etc.)

  # auto-cleanup rule: keep the 5 most recent tagged versions
  cleanup_policies {
    id     = "keep-latest-5"  # policy identifier
    action = "KEEP"           # KEEP means "protect these from deletion"

    # retention criteria
    most_recent_versions {
      keep_count = 5  # number of recent versions to retain per tag
    }
  }

  # auto-cleanup rule: delete all untagged images (dangling layers)
  cleanup_policies {
    id     = "delete-untagged"
    action = "DELETE"          # DELETE means "remove images matching this condition"

    # filter for which images to delete
    condition {
      tag_state = "UNTAGGED"  # targets images with no tag (orphaned build layers)
    }
  }
}
