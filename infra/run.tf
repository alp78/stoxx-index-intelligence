# --------------------------------------------------------------------------
# Cloud Run services and jobs — Dashboard, Pipeline, Setup
# --------------------------------------------------------------------------

locals {
  registry = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.stoxx.repository_id}"
  sql_ip   = google_compute_instance.sql.network_interface[0].network_ip
  sql_user = "sa"
}

# --------------------------------------------------------------------------
# Cloud Run: Dashboard (always-on web service)
# --------------------------------------------------------------------------
resource "google_cloud_run_v2_service" "dashboard" {
  name                = "stoxx-dashboard"
  location            = var.region
  deletion_protection = false

  # Blazor Server requires sticky sessions — WebSocket circuits are instance-bound
  template {
    session_affinity = true
    service_account  = google_service_account.dashboard.email
    timeout          = "3600s"

    scaling {
      min_instance_count = 1
      max_instance_count = 2
    }

    containers {
      image = "${local.registry}/dashboard:latest"

      ports {
        container_port = 8080
      }

      # Blazor Server uses WebSockets (SignalR) — needs HTTP/2 end-to-end
      startup_probe {
        http_get {
          path = "/"
        }
        initial_delay_seconds = 3
        period_seconds        = 10
        failure_threshold     = 3
      }

      env {
        name  = "ConnectionStrings__stoxx"
        value = "Server=${local.sql_ip},1433;Database=stoxx;User Id=${local.sql_user};Password=${var.db_password};TrustServerCertificate=true"
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
      }
    }

    vpc_access {
      network_interfaces {
        network    = google_compute_network.main.id
        subnetwork = google_compute_subnetwork.main.id
      }
      egress = "PRIVATE_RANGES_ONLY"
    }
  }

  depends_on = [google_compute_instance.sql]
}

# Public access (no auth required for dashboard)
resource "google_cloud_run_v2_service_iam_member" "dashboard_public" {
  name     = google_cloud_run_v2_service.dashboard.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "allUsers"
}

# --------------------------------------------------------------------------
# Cloud Run Job: Pipeline (triggered by Airflow)
# --------------------------------------------------------------------------
resource "google_cloud_run_v2_job" "pipeline" {
  name                = "stoxx-pipeline"
  location            = var.region
  deletion_protection = false

  template {
    task_count = 1

    template {
      service_account = google_service_account.pipeline.email
      timeout         = "1800s"
      max_retries     = 1

      containers {
        image = "${local.registry}/pipeline:latest"

        env {
          name  = "SQL_HOST"
          value = local.sql_ip
        }
        env {
          name  = "SQL_PORT"
          value = "1433"
        }
        env {
          name  = "SQL_DATABASE"
          value = "stoxx"
        }
        env {
          name  = "SQL_USER"
          value = local.sql_user
        }
        env {
          name = "SA_PASSWORD"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.db_password.secret_id
              version = "latest"
            }
          }
        }
        env {
          name  = "DD_SERVICE"
          value = "stoxx-pipeline"
        }
        env {
          name  = "DD_ENV"
          value = "prod"
        }
        env {
          name  = "DD_TRACE_AGENT_URL"
          value = "http://${google_compute_instance.airflow.network_interface[0].network_ip}:8126"
        }
        env {
          name = "DD_API_KEY"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.dd_api_key.secret_id
              version = "latest"
            }
          }
        }
        env {
          name  = "LOG_FORMAT"
          value = "json"
        }

        resources {
          limits = {
            cpu    = "2"
            memory = "2Gi"
          }
        }
      }

      vpc_access {
        network_interfaces {
          network    = google_compute_network.main.id
          subnetwork = google_compute_subnetwork.main.id
        }
        egress = "PRIVATE_RANGES_ONLY"
      }
    }
  }

  depends_on = [google_compute_instance.sql]
}

# Cloud Run Job for initial setup (run once)
resource "google_cloud_run_v2_job" "setup" {
  name                = "stoxx-setup"
  location            = var.region
  deletion_protection = false

  template {
    task_count = 1

    template {
      service_account = google_service_account.pipeline.email
      timeout         = "3600s"
      max_retries     = 0

      containers {
        image   = "${local.registry}/pipeline:latest"
        command = ["bash", "-c", "python db/run_ddl.py && python utils/setup_index.py"]

        env {
          name  = "SQL_HOST"
          value = local.sql_ip
        }
        env {
          name  = "SQL_PORT"
          value = "1433"
        }
        env {
          name  = "SQL_DATABASE"
          value = "stoxx"
        }
        env {
          name  = "SQL_USER"
          value = local.sql_user
        }
        env {
          name = "SA_PASSWORD"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.db_password.secret_id
              version = "latest"
            }
          }
        }

        resources {
          limits = {
            cpu    = "2"
            memory = "2Gi"
          }
        }
      }

      vpc_access {
        network_interfaces {
          network    = google_compute_network.main.id
          subnetwork = google_compute_subnetwork.main.id
        }
        egress = "PRIVATE_RANGES_ONLY"
      }
    }
  }

  depends_on = [google_compute_instance.sql]
}
