# --------------------------------------------------------------------------
# Cloud SQL for SQL Server (Express 2022)
# --------------------------------------------------------------------------
resource "google_sql_database_instance" "main" {
  name             = "stoxx-sql"
  database_version = "SQLSERVER_2022_EXPRESS"
  region           = var.region
  root_password    = var.db_password

  depends_on = [google_service_networking_connection.private_vpc]

  settings {
    tier              = var.db_tier
    availability_type = "ZONAL"
    edition           = "ENTERPRISE"

    ip_configuration {
      ipv4_enabled                                  = false
      private_network                               = google_compute_network.main.id
      enable_private_path_for_google_cloud_services = true
    }

    disk_size             = 10
    disk_type             = "PD_SSD"
    disk_autoresize       = true
    disk_autoresize_limit = 50

    backup_configuration {
      enabled                        = true
      start_time                     = "03:00"
      point_in_time_recovery_enabled = true
      transaction_log_retention_days = 7
    }

    maintenance_window {
      day          = 7
      hour         = 4
      update_track = "stable"
    }
  }

  deletion_protection = true
}

resource "google_sql_database" "stoxx" {
  name     = "stoxx"
  instance = google_sql_database_instance.main.name
}
