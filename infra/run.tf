locals {
  registry = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.stoxx.repository_id}"
}

# --------------------------------------------------------------------------
# Cloud Run: Dashboard (always-on web service)
# --------------------------------------------------------------------------
resource "google_cloud_run_v2_service" "dashboard" {
  name                = "stoxx-dashboard"
  location            = var.region
  deletion_protection = false

  template {
    service_account = google_service_account.dashboard.email
    timeout         = "3600s"

    scaling {
      min_instance_count = 0
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
        value = "Server=${google_sql_database_instance.main.private_ip_address},1433;Database=stoxx;User Id=sqlserver;Password=${var.db_password};TrustServerCertificate=true"
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

  depends_on = [google_sql_database.stoxx]
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
          value = google_sql_database_instance.main.private_ip_address
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
          value = "sqlserver"
        }
        env {
          name  = "SA_PASSWORD"
          value = var.db_password
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

  depends_on = [google_sql_database.stoxx]
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
          value = google_sql_database_instance.main.private_ip_address
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
          value = "sqlserver"
        }
        env {
          name  = "SA_PASSWORD"
          value = var.db_password
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

  depends_on = [google_sql_database.stoxx]
}
