# --------------------------------------------------------------------------
# VPC + subnet
# --------------------------------------------------------------------------
resource "google_compute_network" "main" {
  name                    = "stoxx-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "main" {
  name          = "stoxx-subnet"
  ip_cidr_range = "10.0.0.0/24"
  region        = var.region
  network       = google_compute_network.main.id
}

# --------------------------------------------------------------------------
# Private service access (Cloud SQL private IP via VPC peering)
# --------------------------------------------------------------------------
resource "google_compute_global_address" "private_ip_range" {
  name          = "stoxx-private-ip"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = google_compute_network.main.id
}

resource "google_service_networking_connection" "private_vpc" {
  network                 = google_compute_network.main.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_ip_range.name]
}

# --------------------------------------------------------------------------
# Firewall rules
# --------------------------------------------------------------------------
resource "google_compute_firewall" "allow_airflow_ui" {
  name    = "stoxx-allow-airflow"
  network = google_compute_network.main.name

  allow {
    protocol = "tcp"
    ports    = ["8080"]
  }

  # Restrict to IAP source range only
  source_ranges = ["35.235.240.0/20"]
  target_tags   = ["airflow"]
}

resource "google_compute_firewall" "allow_apm" {
  name    = "stoxx-allow-apm"
  network = google_compute_network.main.name

  allow {
    protocol = "tcp"
    ports    = ["8126"]
  }

  # Allow Cloud Run (VPC connector) to reach dd-agent APM port
  source_ranges = ["10.0.0.0/24"]
  target_tags   = ["airflow"]
}

resource "google_compute_firewall" "allow_iap" {
  name    = "stoxx-allow-iap"
  network = google_compute_network.main.name

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  # IAP tunnel source range
  source_ranges = ["35.235.240.0/20"]
  target_tags   = ["airflow"]
}

resource "google_compute_firewall" "deny_all_ingress" {
  name     = "stoxx-deny-all-ingress"
  network  = google_compute_network.main.name
  priority = 65000

  deny {
    protocol = "all"
  }

  source_ranges = ["0.0.0.0/0"]
}
