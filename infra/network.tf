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
# Cloud NAT — allows VMs without public IPs to reach the internet
# --------------------------------------------------------------------------
resource "google_compute_router" "main" {
  name    = "stoxx-router"
  region  = var.region
  network = google_compute_network.main.id
}

resource "google_compute_router_nat" "main" {
  name                               = "stoxx-nat"
  router                             = google_compute_router.main.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
}

# --------------------------------------------------------------------------
# Firewall rules
# --------------------------------------------------------------------------
resource "google_compute_firewall" "allow_sql" {
  name    = "stoxx-allow-sql"
  network = google_compute_network.main.name

  allow {
    protocol = "tcp"
    ports    = ["1433"]
  }

  # Allow Cloud Run + Airflow (same subnet) to reach SQL Server VM
  source_ranges = ["10.0.0.0/24"]
  target_tags   = ["sql"]
}

resource "google_compute_firewall" "allow_airflow_ui" {
  name    = "stoxx-allow-airflow"
  network = google_compute_network.main.name

  allow {
    protocol = "tcp"
    ports    = ["8080"]
  }

  # IAP range + optional admin IP (from tfvars, not committed)
  source_ranges = compact(concat(
    ["35.235.240.0/20"],
    var.admin_ip != "" ? ["${var.admin_ip}/32"] : []
  ))
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
    ports    = ["22", "1433"]
  }

  # IAP tunnel source range
  source_ranges = ["35.235.240.0/20"]
  target_tags   = ["airflow", "sql"]
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
