# --------------------------------------------------------------------------
# Airflow VM (LocalExecutor + PostgreSQL on COS)
# --------------------------------------------------------------------------
resource "google_compute_instance" "airflow" {
  name         = "stoxx-airflow"
  machine_type = "e2-medium"
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
    startup-script = file("${path.module}/scripts/airflow-startup.sh")
  }

  allow_stopping_for_update = true
}
