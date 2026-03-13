# --------------------------------------------------------------------------
# Cloud Run services and jobs — Dashboard, Pipeline, Setup
# --------------------------------------------------------------------------

# computed values derived from other resources (not settable via tfvars)
locals {
  registry = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.stoxx.repository_id}"  # full Artifact Registry URL
  sql_ip   = google_compute_instance.sql.network_interface[0].network_ip  # private IP of the SQL VM (resolved after VM creation)
  sql_user = "sa"  # SQL Server login username
}

# --------------------------------------------------------------------------
# Cloud Run: Dashboard (always-on web service)
# --------------------------------------------------------------------------

# creates the Cloud Run service for the Blazor Server dashboard
resource "google_cloud_run_v2_service" "dashboard" {
  name                = "stoxx-dashboard"  # service name in GCP
  location            = var.region         # region where the service runs
  deletion_protection = false              # allows terraform destroy without manual confirmation

  # revision template: blueprint for every container instance
  # Blazor Server requires sticky sessions — WebSocket circuits are instance-bound
  template {
    session_affinity = true                                    # routes repeat requests to same instance (required for Blazor WebSocket circuits)
    service_account  = google_service_account.dashboard.email  # runtime identity for this service
    timeout          = "3600s"                                 # max request duration (1 hour, needed for long-lived WebSocket connections)

    # auto-scaling configuration
    scaling {
      min_instance_count = 0  # scale to zero when idle (saves cost)
      max_instance_count = 2  # max instances for handling traffic spikes
    }

    # container spec (image, ports, env vars, probes, resource limits)
    containers {
      image = "${local.registry}/dashboard:latest"  # Docker image to run

      # which port the container listens on
      ports {
        container_port = 8080
      }

      # health check to determine when the container is ready
      startup_probe {
        # probe sends HTTP GET to this path
        http_get {
          path = "/"
        }
        initial_delay_seconds = 3   # wait 3s after container starts before first probe
        period_seconds        = 10  # check every 10s after that
        failure_threshold     = 3   # after 3 consecutive failures, kill and replace the container
      }

      # connection string without password (password injected separately via Secret Manager)
      env {
        name  = "ConnectionStrings__stoxx"
        value = "Server=${local.sql_ip},1433;Database=stoxx;User Id=${local.sql_user};TrustServerCertificate=true"
      }
      # DB password pulled from Secret Manager at runtime (never in Terraform state)
      env {
        name = "DB_PASSWORD"
        # references an external secret instead of a plaintext value
        value_source {
          # pointer to a specific secret and version
          secret_key_ref {
            secret  = google_secret_manager_secret.db_password.secret_id
            version = "latest"  # always resolves to the newest secret version
          }
        }
      }

      # CPU and memory limits for the container
      resources {
        limits = {  # ceiling (not allocation); Cloud Run bills actual usage
          cpu    = "1"
          memory = "512Mi"
        }
      }
    }

    # connects Cloud Run to the private VPC (required to reach SQL VM)
    vpc_access {
      # direct VPC egress (no Serverless VPC Connector needed)
      network_interfaces {
        network    = google_compute_network.main.id
        subnetwork = google_compute_subnetwork.main.id
      }
      egress = "PRIVATE_RANGES_ONLY"  # only route private IP traffic through VPC (public traffic goes direct)
    }
  }

  depends_on = [google_compute_instance.sql]  # wait for SQL VM to exist before creating this service (provisioning order only)
}

# grants public access to the dashboard (no authentication required)
resource "google_cloud_run_v2_service_iam_member" "dashboard_public" {
  name     = google_cloud_run_v2_service.dashboard.name  # the Cloud Run service to apply the binding to
  location = var.region
  role     = "roles/run.invoker"  # allows calling the service
  member   = "allUsers"           # anyone on the internet
}

# --------------------------------------------------------------------------
# Cloud Run Job: Pipeline (triggered by Airflow)
# --------------------------------------------------------------------------

# creates the Cloud Run job for the data pipeline
resource "google_cloud_run_v2_job" "pipeline" {
  name                = "stoxx-pipeline"
  location            = var.region
  deletion_protection = false

  # execution template (how many tasks to run per execution)
  template {
    task_count = 1  # number of parallel containers per execution (1 = single run)

    # task template (what each container looks like)
    template {
      service_account = google_service_account.pipeline.email  # runtime identity for the pipeline container
      timeout         = "1800s"  # max execution time (30 minutes)
      max_retries     = 1        # retry once on failure before marking as failed

      containers {
        image = "${local.registry}/pipeline:latest"  # Docker image to run

        # SQL Server connection details (individual vars, not a connection string)
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
        # DB password from Secret Manager
        env {
          name = "SA_PASSWORD"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.db_password.secret_id
              version = "latest"
            }
          }
        }
        # Datadog APM configuration
        env {
          name  = "DD_SERVICE"
          value = "stoxx-pipeline"  # service name shown in Datadog APM
        }
        env {
          name  = "DD_ENV"
          value = "prod"  # environment tag in Datadog
        }
        env {
          name  = "DD_TRACE_AGENT_URL"
          value = "http://${google_compute_instance.airflow.network_interface[0].network_ip}:8126"  # Datadog agent endpoint on the Airflow VM
        }
        # Datadog API key from Secret Manager
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
          value = "json"  # structured logging for Datadog ingestion
        }

        # CPU and memory limits
        resources {
          limits = {
            cpu    = "2"
            memory = "2Gi"
          }
        }
      }

      # connects to private VPC for SQL VM and Datadog agent access
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

# --------------------------------------------------------------------------
# Cloud Run Job: Setup (run once to initialize DB schema and seed index data)
# --------------------------------------------------------------------------

# creates the one-time setup job (runs DDL scripts + index setup)
resource "google_cloud_run_v2_job" "setup" {
  name                = "stoxx-setup"
  location            = var.region
  deletion_protection = false

  # execution template
  template {
    task_count = 1

    # task template
    template {
      service_account = google_service_account.pipeline.email
      timeout         = "3600s"  # 1 hour (setup includes historical data backfill)
      max_retries     = 0        # no retries (setup is idempotent, just re-execute manually)

      containers {
        image   = "${local.registry}/pipeline:latest"                                    # same pipeline image, different entrypoint
        command = ["bash", "-c", "python db/run_ddl.py && python utils/setup_index.py"]  # overrides default entrypoint to run DDL + setup scripts

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
        # DB password from Secret Manager
        env {
          name = "SA_PASSWORD"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.db_password.secret_id
              version = "latest"
            }
          }
        }

        # CPU and memory limits
        resources {
          limits = {
            cpu    = "2"
            memory = "2Gi"
          }
        }
      }

      # connects to private VPC for SQL VM access
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
