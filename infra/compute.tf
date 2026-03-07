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
    startup-script = replace(file("${path.module}/scripts/airflow-startup.sh"), "\r\n", "\n")
    enable-oslogin = "TRUE"
  }

  shielded_instance_config {
    enable_secure_boot          = true
    enable_vtpm                 = true
    enable_integrity_monitoring = true
  }

  allow_stopping_for_update = true
}

# --------------------------------------------------------------------------
# SQL Server VM (replaces Cloud SQL managed instance)
# --------------------------------------------------------------------------
resource "google_compute_instance" "sql" {
  name         = "stoxx-sql"
  machine_type = "e2-medium"
  zone         = var.zone
  tags         = ["sql"]

  boot_disk {
    initialize_params {
      image = "projects/ubuntu-os-cloud/global/images/family/ubuntu-2204-lts"
      size  = 30
      type  = "pd-ssd"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.main.id
    # No public IP — access via IAP tunnel only
  }

  service_account {
    email  = google_service_account.pipeline.email
    scopes = ["cloud-platform"]
  }

  metadata = {
    startup-script = replace(file("${path.module}/scripts/sql-startup.sh"), "\r\n", "\n")
    sa-password    = var.db_password
    dd-api-key     = var.dd_api_key
    enable-oslogin = "TRUE"
  }

  shielded_instance_config {
    enable_secure_boot          = true
    enable_vtpm                 = true
    enable_integrity_monitoring = true
  }

  allow_stopping_for_update = true
}
