# --------------------------------------------------------------------------
# Airflow VM (Docker Compose on e2-small)
# --------------------------------------------------------------------------
resource "google_compute_instance" "airflow" {
  name         = "stoxx-airflow"
  machine_type = "e2-small"
  zone         = var.zone
  tags         = ["airflow"]

  boot_disk {
    initialize_params {
      image = "projects/cos-cloud/global/images/family/cos-stable"
      size  = 20
      type  = "pd-balanced"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.main.id

    access_config {
      # Ephemeral public IP for SSH + Airflow UI
    }
  }

  service_account {
    email  = google_service_account.airflow.email
    scopes = ["cloud-platform"]
  }

  metadata = {
    gce-container-declaration = yamlencode({
      spec = {
        containers = [{
          image = "apache/airflow:2.10-python3.12"
          env = [
            { name = "AIRFLOW__CORE__LOAD_EXAMPLES", value = "false" },
            { name = "AIRFLOW__CORE__EXECUTOR",      value = "SequentialExecutor" },
          ]
          command = ["bash", "-c", "airflow db init && airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@local && airflow standalone"]
          ports   = [{ containerPort = 8080 }]
          volumeMounts = [{
            name      = "dags"
            mountPath = "/opt/airflow/dags"
          }]
        }]
        volumes = [{
          name = "dags"
          hostPath = { path = "/home/airflow/dags" }
        }]
      }
    })
  }

  allow_stopping_for_update = true
}
