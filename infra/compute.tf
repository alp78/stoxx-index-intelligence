# --------------------------------------------------------------------------
# Airflow VM (LocalExecutor + PostgreSQL on COS)
# --------------------------------------------------------------------------

# creates the Airflow GCE VM running Container-Optimized OS
resource "google_compute_instance" "airflow" {
  name         = "stoxx-airflow"  # VM instance name in GCP
  machine_type = "e2-medium"      # 2 vCPU, 4 GB RAM (shared-core)
  zone         = var.zone         # zone where the VM is created
  tags         = ["airflow"]      # network tags used by firewall rules to target this VM

  # the VM's primary disk
  boot_disk {
    # disk image, size, and type for first creation
    initialize_params {
      image = "projects/cos-cloud/global/images/family/cos-stable"  # Container-Optimized OS (Docker pre-installed, read-only filesystem)
      size  = 20              # disk size in GB
      type  = "pd-balanced"   # SSD-backed, lower cost than pd-ssd
    }
  }

  # attaches VM to the VPC subnet
  network_interface {
    subnetwork = google_compute_subnetwork.main.id  # subnet this VM connects to

    # assigns an ephemeral public IP for SSH + Airflow UI
    access_config {}
  }

  # GCP identity the VM runs as
  service_account {
    email  = google_service_account.airflow.email  # email of the service account
    scopes = ["cloud-platform"]                    # grants access to all APIs (IAM controls actual permissions)
  }

  # key-value pairs passed to the VM instance
  metadata = {
    startup-script = replace(file("${path.module}/scripts/airflow-startup.sh"), "\r\n", "\n")  # shell script executed on every boot (replace normalizes Windows line endings)
    dd-api-key     = var.dd_api_key   # Datadog API key read by the startup script to configure the agent
    enable-oslogin = "TRUE"           # enforces IAM-based SSH access instead of SSH keys in metadata
  }

  # hardware security features
  shielded_instance_config {
    enable_secure_boot          = true  # only verified bootloader and kernel can run
    enable_vtpm                 = true  # virtual Trusted Platform Module for measured boot
    enable_integrity_monitoring = true  # alerts if boot sequence is tampered with
  }

  allow_stopping_for_update = true  # allows Terraform to stop the VM before applying changes
}

# --------------------------------------------------------------------------
# SQL Server VM (replaces Cloud SQL managed instance)
# --------------------------------------------------------------------------

# creates the SQL Server GCE VM running Ubuntu 22.04
resource "google_compute_instance" "sql" {
  name         = "stoxx-sql"    # VM instance name in GCP
  machine_type = "e2-small"     # 2 vCPU, 2 GB RAM (shared-core, smallest for SQL Server)
  zone         = var.zone       # zone where the VM is created
  tags         = ["sql"]        # network tags used by firewall rules to target this VM

  # the VM's primary disk
  boot_disk {
    # disk image, size, and type for first creation
    initialize_params {
      image = "projects/ubuntu-os-cloud/global/images/family/ubuntu-2204-lts"  # Ubuntu 22.04 LTS (required for SQL Server on Linux)
      size  = 30          # disk size in GB
      type  = "pd-ssd"    # SSD for database I/O performance
    }
  }

  # attaches VM to the VPC subnet — no access_config = no public IP, access via IAP tunnel only
  network_interface {
    subnetwork = google_compute_subnetwork.main.id  # subnet this VM connects to
  }

  # GCP identity the VM runs as (reuses pipeline SA)
  service_account {
    email  = google_service_account.pipeline.email  # email of the service account
    scopes = ["cloud-platform"]                     # grants access to all APIs (IAM controls actual permissions)
  }

  # key-value pairs passed to the VM instance
  metadata = {
    startup-script = replace(file("${path.module}/scripts/sql-startup.sh"), "\r\n", "\n")  # installs and configures SQL Server + Datadog agent on boot
    sa-password    = var.db_password   # SQL Server SA password read by the startup script
    dd-api-key     = var.dd_api_key   # Datadog API key read by the startup script
    enable-oslogin = "TRUE"           # enforces IAM-based SSH access
  }

  # hardware security features
  shielded_instance_config {
    enable_secure_boot          = true  # only verified bootloader and kernel can run
    enable_vtpm                 = true  # virtual Trusted Platform Module for measured boot
    enable_integrity_monitoring = true  # alerts if boot sequence is tampered with
  }

  allow_stopping_for_update = true  # allows Terraform to stop the VM before applying changes
}
