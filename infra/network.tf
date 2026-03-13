# --------------------------------------------------------------------------
# VPC + subnet
# --------------------------------------------------------------------------

# creates the project VPC network
resource "google_compute_network" "main" {
  name                    = "stoxx-vpc"  # network name in GCP
  auto_create_subnetworks = false        # disables auto-creation of subnets (we define our own below)
}

# creates a subnet inside the VPC
resource "google_compute_subnetwork" "main" {
  name          = "stoxx-subnet"                    # subnet name in GCP
  ip_cidr_range = "10.0.0.0/24"                     # CIDR range — 256 IPs for all VMs and Cloud Run VPC connectors
  region        = var.region                         # region the subnet lives in
  network       = google_compute_network.main.id     # reference to the parent VPC network
}

# --------------------------------------------------------------------------
# Cloud NAT — allows VMs without public IPs to reach the internet
# --------------------------------------------------------------------------

# creates a Cloud Router (required by Cloud NAT)
resource "google_compute_router" "main" {
  name    = "stoxx-router"
  region  = var.region
  network = google_compute_network.main.id  # the VPC network this router is attached to
}

# creates Cloud NAT for outbound internet from private VMs (e.g., SQL VM)
resource "google_compute_router_nat" "main" {
  name                               = "stoxx-nat"
  router                             = google_compute_router.main.name          # reference to the Cloud Router
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"                              # auto-allocate external IPs for NAT (no static IP needed)
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"          # apply NAT to all subnets and IP ranges in the VPC
}

# --------------------------------------------------------------------------
# Firewall rules
# --------------------------------------------------------------------------

# allows SQL Server traffic (port 1433) within the subnet
resource "google_compute_firewall" "allow_sql" {
  name    = "stoxx-allow-sql"
  network = google_compute_network.main.name  # the VPC network this rule applies to

  # permitted protocol and ports
  allow {
    protocol = "tcp"
    ports    = ["1433"]
  }

  source_ranges = ["10.0.0.0/24"]  # who can connect (subnet CIDR = Cloud Run + Airflow)
  target_tags   = ["sql"]          # which VMs this rule applies to (VMs tagged "sql")
}

# allows Airflow UI access (port 8080) from IAP + admin IP
resource "google_compute_firewall" "allow_airflow_ui" {
  name    = "stoxx-allow-airflow"
  network = google_compute_network.main.name

  # permitted protocol and ports
  allow {
    protocol = "tcp"
    ports    = ["8080"]
  }

  # IAP range (required) + optional admin IP for direct browser access
  # compact(concat(...)) merges two lists and removes empty strings
  source_ranges = compact(concat(
    ["35.235.240.0/20"],
    var.admin_ip != "" ? ["${var.admin_ip}/32"] : []
  ))
  target_tags = ["airflow"]
}

# allows APM traces (port 8126) from Cloud Run to Datadog agent on Airflow VM
resource "google_compute_firewall" "allow_apm" {
  name    = "stoxx-allow-apm"
  network = google_compute_network.main.name

  # permitted protocol and ports
  allow {
    protocol = "tcp"
    ports    = ["8126"]
  }

  source_ranges = ["10.0.0.0/24"]  # subnet CIDR (Cloud Run VPC connector sends traces here)
  target_tags   = ["airflow"]
}

# allows IAP tunnel access (SSH + SQL) to both VMs
resource "google_compute_firewall" "allow_iap" {
  name    = "stoxx-allow-iap"
  network = google_compute_network.main.name

  # permitted protocol and ports
  allow {
    protocol = "tcp"
    ports    = ["22", "1433"]  # SSH (22) for terminal access, SQL (1433) for IAP-tunneled DB connections
  }

  source_ranges = ["35.235.240.0/20"]  # Google IAP tunnel IP range
  target_tags   = ["airflow", "sql"]   # applies to both Airflow and SQL VMs
}

# default deny-all ingress (catch-all, lowest priority)
resource "google_compute_firewall" "deny_all_ingress" {
  name     = "stoxx-deny-all-ingress"
  network  = google_compute_network.main.name
  priority = 65000  # very low priority, so all allow rules above take precedence

  # blocks all protocols and ports
  deny {
    protocol = "all"
  }

  source_ranges = ["0.0.0.0/0"]  # matches all traffic from anywhere
}
