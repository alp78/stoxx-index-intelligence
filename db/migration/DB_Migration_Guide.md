# Database Migration Guide — Cloud SQL to SQL Server on GCE VM

*Step-by-step migration of the `stoxx` database from Cloud SQL (managed) to a self-hosted SQL Server 2022 instance on a GCE VM.*

**Branch: `feat/migrate-db-to-vm-sql` | Region: europe-west1 | Zone: europe-west1-b**

---

## Table of Contents

1. [Motivation](#1-motivation)
2. [Architecture: Before & After](#2-architecture-before--after)
3. [Step 1: Provision SQL VM alongside Cloud SQL](#3-step-1-provision-sql-vm-alongside-cloud-sql)
   - 3.1 Terraform Changes
   - 3.2 The `use_sql_vm` Toggle
   - 3.3 Apply & Verify
4. [Step 2: Install SQL Server on the VM](#4-step-2-install-sql-server-on-the-vm)
   - 4.1 SSH into the VM
   - 4.2 Startup Script & Cloud NAT Issue
   - 4.3 GPG Key Fix
   - 4.4 Install SQL Server
   - 4.5 Configure SQL Server
5. [Step 3: Create Database & Run DDL](#5-step-3-create-database--run-ddl)
6. [Step 4: Migrate Data](#6-step-4-migrate-data)
7. [Step 5: Flip the Switch](#7-step-5-flip-the-switch)
8. [Step 5b: Firewall — Local PC Access](#8-step-5b-firewall--local-pc-access)
9. [Step 6: Decommission Cloud SQL](#9-step-6-decommission-cloud-sql)
10. [Troubleshooting](#10-troubleshooting)

---

## 1. Motivation

Cloud SQL for SQL Server has limitations:
- `enable_private_path_for_google_cloud_services` is **not supported** for SQL Server (only PostgreSQL/MySQL)
- Minimum tier `db-custom-1-3840` is expensive for a small workload
- Less control over configuration, backups, and maintenance windows
- SQL Server Express on Cloud SQL still uses ENTERPRISE edition pricing

A self-hosted VM running SQL Server 2022 Developer edition gives full features at zero license cost, with complete control over the instance.

---

## 2. Architecture: Before & After

### Before (Cloud SQL)

```
Cloud Run (dashboard/pipeline)
  --> VPC peering --> Cloud SQL (stoxx-sql, private IP)
```

- Cloud SQL managed instance with private IP via VPC peering
- `google_service_networking_connection` for private service access
- `roles/cloudsql.client` IAM bindings on service accounts
- Admin user: `sqlserver`

### After (GCE VM)

```
Cloud Run (dashboard/pipeline)
  --> VPC subnet (10.0.0.0/24) --> GCE VM (stoxx-sql, internal IP)
```

- SQL Server 2022 Developer on Ubuntu 22.04 LTS
- Same VPC/subnet, direct TCP connection on port 1433
- No VPC peering needed, no Cloud SQL IAM
- Cloud NAT for outbound internet (package updates)
- Admin user: `sa`

---

## 3. Step 1: Provision SQL VM alongside Cloud SQL

### 3.1 Terraform Changes

All changes are in `infra/`. The key design: **both instances coexist** during migration, with a variable controlling which one services connect to.

#### Files modified

| File | Changes |
|------|---------|
| `compute.tf` | Added `google_compute_instance.sql` — the VM resource |
| `network.tf` | Added Cloud NAT (router + NAT gateway), firewall rule for port 1433, IAP SSH for `sql` tag |
| `run.tf` | Conditional locals (`sql_ip`, `sql_user`) driven by `use_sql_vm` variable |
| `sql.tf` | Cloud SQL kept intact with TODO markers |
| `iam.tf` | `roles/cloudsql.client` kept with TODO markers |
| `variables.tf` | Added `use_sql_vm` bool (default: `false`) |
| `outputs.tf` | Added `sql_vm_ip` and `sql_cloud_ip` outputs |

#### Files created

| File | Purpose |
|------|---------|
| `scripts/sql-startup.sh` | Bootstrap script: installs SQL Server 2022 on first boot |

#### VM resource (`compute.tf`)

```hcl
resource "google_compute_instance" "sql" {
  name         = "stoxx-sql"
  machine_type = "e2-medium"        # 2 vCPU, 4 GB RAM
  zone         = var.zone            # europe-west1-b
  tags         = ["sql"]             # targeted by firewall rules

  boot_disk {
    initialize_params {
      image = "projects/ubuntu-os-cloud/global/images/family/ubuntu-2204-lts"
      size  = 30                     # GB
      type  = "pd-ssd"              # SSD for database I/O
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.main.id
    # No public IP — access via IAP tunnel only
  }

  metadata = {
    startup-script = replace(file("${path.module}/scripts/sql-startup.sh"), "\r\n", "\n")
    sa-password    = var.db_password  # passed to startup script via metadata API
    enable-oslogin = "TRUE"
  }
}
```

Key decisions:
- **No public IP** — the VM is only reachable via the VPC (from Cloud Run) or via IAP tunnel (for SSH admin)
- **`pd-ssd`** — SSD boot disk for database performance
- **SA password in metadata** — the startup script reads it via `curl` to the metadata server. In production, consider Secret Manager instead.
- **`replace(\r\n, \n)`** — Windows-to-Unix line ending conversion (repo is on Windows)

#### Cloud NAT (`network.tf`)

```hcl
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
```

**Why Cloud NAT is needed:** The SQL VM has no public IP, so it cannot reach the internet to download packages (`apt-get`, Microsoft repos). Cloud NAT provides outbound-only internet access for all VMs in the VPC without assigning public IPs.

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `nat_ip_allocate_option` | `AUTO_ONLY` | GCP auto-assigns a NAT IP |
| `source_subnetwork_ip_ranges_to_nat` | `ALL_SUBNETWORKS_ALL_IP_RANGES` | All VMs in all subnets get NAT |

#### Firewall rule (`network.tf`)

```hcl
resource "google_compute_firewall" "allow_sql" {
  name    = "stoxx-allow-sql"
  network = google_compute_network.main.name
  allow {
    protocol = "tcp"
    ports    = ["1433"]
  }
  source_ranges = ["10.0.0.0/24"]   # subnet CIDR
  target_tags   = ["sql"]            # only VMs tagged "sql"
}
```

This allows Cloud Run services (which egress via the VPC subnet) and the Airflow VM to reach SQL Server on port 1433. The existing `deny_all_ingress` rule (priority 65000) blocks everything else.

The IAP SSH rule was also updated to include the `sql` tag:
```hcl
target_tags = ["airflow", "sql"]    # was: ["airflow"]
```

### 3.2 The `use_sql_vm` Toggle

The connection routing is controlled by a single variable in `variables.tf`:

```hcl
variable "use_sql_vm" {
  description = "When true, route all services to the SQL VM instead of Cloud SQL"
  type        = bool
  default     = false
}
```

In `run.tf`, locals resolve the active IP and user:

```hcl
locals {
  sql_vm_ip    = google_compute_instance.sql.network_interface[0].network_ip
  sql_cloud_ip = google_sql_database_instance.main.private_ip_address
  sql_ip       = var.use_sql_vm ? local.sql_vm_ip : local.sql_cloud_ip
  sql_user     = var.use_sql_vm ? "sa" : "sqlserver"
}
```

All connection strings and env vars reference `local.sql_ip` and `local.sql_user`, so flipping the switch is a single `terraform apply -var="use_sql_vm=true"`.

### 3.3 Apply & Verify

```bash
cd infra
terraform plan     # review: should add VM, firewall, NAT — no changes to Cloud SQL
terraform apply    # creates resources, dashboard stays on Cloud SQL
terraform output sql_vm_ip      # note the VM's internal IP
terraform output sql_cloud_ip   # confirm Cloud SQL still active
```

---

## 4. Step 2: Install SQL Server on the VM

### 4.1 SSH into the VM

```bash
gcloud compute ssh stoxx-sql --zone=europe-west1-b --tunnel-through-iap
```

| Flag | Purpose |
|------|---------|
| `--zone=europe-west1-b` | GCE zone where the VM lives |
| `--tunnel-through-iap` | Routes SSH through Identity-Aware Proxy — no public IP needed. Requires the IAP firewall rule (`35.235.240.0/20` on port 22) |

On first connect, GCE creates a home directory:
```
Creating directory '/home/youruser_gmail_com'.
```

### 4.2 Startup Script & Cloud NAT Issue

**Problem:** The startup script (`sql-startup.sh`) runs automatically on first boot via GCE's `google-startup-scripts` service, but the VM was created **before** Cloud NAT was deployed. Without NAT, the VM had no outbound internet access.

**Symptom:**
```
startup-script: curl: (28) Failed to connect to packages.microsoft.com port 443
after 216425 ms: Connection timed out
startup-script: gpg: no valid OpenPGP data found.
Script "startup-script" failed with error: exit status 2
```

**Diagnosis:**
```bash
# Check startup script logs
sudo journalctl -u google-startup-scripts --no-pager | tail -30

# Check if SQL Server is installed
systemctl status mssql-server
# Output: Unit mssql-server.service could not be found.
```

**Fix:** Deploy Cloud NAT via Terraform, then re-run the startup script manually.

**Verification that NAT works:**
```bash
curl -s --max-time 10 https://packages.microsoft.com > /dev/null && echo "OK" || echo "FAIL"
# Output: OK
```

### 4.3 GPG Key Fix

**Problem:** The startup script downloads the Microsoft GPG key and repo list files, but the `.list` files from Microsoft's CDN use the legacy format without `signed-by`. Ubuntu 22.04+ requires explicit keyring references.

**Symptom:**
```
W: GPG error: https://packages.microsoft.com/ubuntu/22.04/prod jammy InRelease:
The following signatures couldn't be verified because the public key is not
available: NO_PUBKEY EB3E94ADBE1229CF
E: The repository '...' is not signed.
```

Then:
```
E: Unable to locate package mssql-server
```

**Root cause:** The key file exists at `/usr/share/keyrings/microsoft-prod.gpg`, but the `.list` files don't reference it with `signed-by=`, so apt ignores the repos.

**Fix:** Overwrite the repo list files with the correct format:

```bash
# Import the GPG key (may already exist from first run)
curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
  | sudo gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
```

| Component | Purpose |
|-----------|---------|
| `curl -fsSL` | `-f` fail silently on HTTP errors, `-s` silent, `-S` show errors, `-L` follow redirects |
| `gpg --dearmor` | Converts ASCII-armored key to binary format (required by apt) |
| `-o /usr/share/keyrings/...` | Standard location for third-party keyring files |

```bash
# Write repo files with signed-by directive
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] \
https://packages.microsoft.com/ubuntu/22.04/mssql-server-2022 jammy main" \
  | sudo tee /etc/apt/sources.list.d/mssql-server-2022.list

echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] \
https://packages.microsoft.com/ubuntu/22.04/prod jammy main" \
  | sudo tee /etc/apt/sources.list.d/mssql-release.list
```

| Component | Purpose |
|-----------|---------|
| `deb [arch=amd64 signed-by=...]` | APT source with architecture filter and explicit keyring |
| `mssql-server-2022` | SQL Server engine packages |
| `prod` | Microsoft tools packages (sqlcmd, bcp, ODBC driver) |
| `jammy` | Ubuntu 22.04 codename |
| `tee` | Writes stdin to file (needs sudo for `/etc/apt/`) |

**The startup script was also fixed** (`infra/scripts/sql-startup.sh`) to use the `signed-by` format for future deployments.

### 4.4 Install SQL Server

```bash
sudo apt-get update -y
sudo ACCEPT_EULA=Y apt-get install -y mssql-server mssql-tools18 unixodbc-dev
```

| Package | Purpose |
|---------|---------|
| `mssql-server` | SQL Server 2022 engine |
| `mssql-tools18` | Command-line tools: `sqlcmd` (queries), `bcp` (bulk copy) |
| `unixodbc-dev` | ODBC driver headers (required by pyodbc in the pipeline) |
| `ACCEPT_EULA=Y` | Required by Microsoft packages — auto-accepts license |

### 4.5 Configure SQL Server

```bash
# Read SA password from VM metadata
SA_PWD=$(curl -s -H "Metadata-Flavor: Google" \
  http://metadata.google.internal/computeMetadata/v1/instance/attributes/sa-password)
```

| Component | Purpose |
|-----------|---------|
| `-H "Metadata-Flavor: Google"` | Required header for GCE metadata API (prevents SSRF) |
| `computeMetadata/v1/instance/attributes/sa-password` | Reads the `sa-password` key set in Terraform `metadata` block |

```bash
# Configure SQL Server (Developer edition)
sudo MSSQL_SA_PASSWORD="$SA_PWD" MSSQL_PID="Developer" \
  /opt/mssql/bin/mssql-conf setup accept-eula
```

| Variable | Value | Purpose |
|----------|-------|---------|
| `MSSQL_SA_PASSWORD` | From metadata | System administrator password |
| `MSSQL_PID` | `Developer` | Edition — Developer has all Enterprise features, free for non-production. Also fine for small production workloads with no licensing requirement. |

```bash
# Enable on boot and start
sudo systemctl enable mssql-server
sudo systemctl restart mssql-server
```

| Command | Purpose |
|---------|---------|
| `systemctl enable` | Auto-start SQL Server on VM reboot |
| `systemctl restart` | Start (or restart) the service now |

**Verify:**

```bash
/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$SA_PWD" -C -Q "SELECT @@VERSION"
```

| Flag | Purpose |
|------|---------|
| `-S localhost` | Server address (local) |
| `-U sa` | Login as system administrator |
| `-P "$SA_PWD"` | Password |
| `-C` | Trust server certificate (self-signed) |
| `-Q "..."` | Execute query and exit |

Expected output:
```
Microsoft SQL Server 2022 (RTM-CU23) (KB5078297) - 16.0.4236.2 (X64)
        Jan 22 2026 17:50:56
        Copyright (C) 2022 Microsoft Corporation
        Developer Edition (64-bit) on Linux (Ubuntu 22.04.5 LTS) <X64>
```

### 4.6 Post-Install: Marker File & PATH

```bash
# Mark installation as complete — prevents the startup script from
# re-running the full install on every VM reboot. On subsequent boots,
# the script checks for this file and simply starts the service.
sudo touch /var/lib/sql-setup-done
```

```bash
# Add sqlcmd/bcp to the system PATH so all users can run them
# without typing the full /opt/mssql-tools18/bin/ prefix.
echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' | sudo tee /etc/profile.d/mssql.sh
```

| Component | Purpose |
|-----------|---------|
| `/etc/profile.d/mssql.sh` | Shell snippet sourced on every login — makes `sqlcmd` and `bcp` available system-wide |
| `sudo tee` | Writes to a root-owned directory while reading from stdin (piped from `echo`) |

```bash
# Load the PATH change into the current session (otherwise only
# takes effect on next login).
source /etc/profile.d/mssql.sh
```

After this, `sqlcmd` and `bcp` work without the full path:
```bash
sqlcmd -S localhost -U sa -P "$SA_PWD" -C -Q "SELECT 1"
```

---

## 5. Step 3: Create Database & Run DDL

### 5.1 Copy DDL scripts to the VM

From your **local machine** (not the VM), use `gcloud compute scp` to copy the
SQL scripts via IAP tunnel. This is the same secure channel used for SSH — no
public IP or open ports required.

```bash
gcloud compute scp \
  db/ddl/bronze_schema.sql \
  db/ddl/silver_schema.sql \
  db/ddl/gold_schema.sql \
  db/seed/countries.sql \
  stoxx-sql:/tmp/ \
  --zone=europe-west1-b --tunnel-through-iap
```

| Component | Purpose |
|-----------|---------|
| `gcloud compute scp` | Secure copy to/from GCE instances — wraps SCP over gcloud SSH |
| `stoxx-sql:/tmp/` | Destination: `<vm-name>:<path>`. `/tmp/` is writable by all users and cleaned on reboot |
| `--zone=europe-west1-b` | Required to locate the VM instance |
| `--tunnel-through-iap` | Routes the SCP connection through Identity-Aware Proxy, same as SSH |

The four files are the complete schema definition:

| File | Contents |
|------|----------|
| `bronze_schema.sql` | Creates the `stoxx` database (idempotent) + bronze layer tables (raw data, 1:1 with source JSON) |
| `silver_schema.sql` | Silver layer tables (cleaned, gap-filled, deduplicated) |
| `gold_schema.sql` | Gold layer tables (scores, performance, aggregates — what the dashboard reads) |
| `countries.sql` | Seed data: country code → country name mapping |

### 5.2 Set up the SA password variable

On the **VM**, read the SA password from instance metadata into a shell variable.
This avoids hardcoding the password in commands.

```bash
SA_PWD=$(curl -s -H "Metadata-Flavor: Google" \
  http://metadata.google.internal/computeMetadata/v1/instance/attributes/sa-password)
```

### 5.3 Run DDL scripts in order

The scripts must run in order: bronze first (creates the database), then silver
and gold (create tables within it), then the seed data.

```bash
# Bronze schema — runs against master, creates the stoxx database,
# then creates bronze.* tables. The script includes its own USE/GO
# statements so no -d flag is needed.
sqlcmd -S localhost -U sa -P "$SA_PWD" -C -i /tmp/bronze_schema.sql
```

```bash
# Silver schema — creates silver.* tables within the stoxx database.
# -d stoxx tells sqlcmd to connect directly to that database.
sqlcmd -S localhost -U sa -P "$SA_PWD" -C -d stoxx -i /tmp/silver_schema.sql
```

```bash
# Gold schema — creates gold.* tables (scores_daily, index_performance, etc.)
sqlcmd -S localhost -U sa -P "$SA_PWD" -C -d stoxx -i /tmp/gold_schema.sql
```

```bash
# Countries seed — populates the ref.countries lookup table used by
# the dashboard for country flag display.
sqlcmd -S localhost -U sa -P "$SA_PWD" -C -d stoxx -i /tmp/countries.sql
```

| Flag | Purpose |
|------|---------|
| `-S localhost` | Connect to the local SQL Server instance |
| `-U sa` | Authenticate as the system administrator |
| `-P "$SA_PWD"` | Password (read from metadata in step 5.2) |
| `-C` | Trust the server's self-signed TLS certificate |
| `-i /tmp/file.sql` | Execute the SQL script from the given file path |
| `-d stoxx` | Set the initial database context (equivalent to `USE stoxx`) — omitted for bronze because that script creates the database itself |

All scripts are **idempotent** — they use `IF NOT EXISTS` guards, so re-running
them is safe and produces no errors.

### 5.4 Verify

Confirm schemas and tables were created:

```bash
sqlcmd -S localhost -U sa -P "$SA_PWD" -C -d stoxx -Q \
  "SELECT s.name AS [schema], t.name AS [table] FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id ORDER BY s.name, t.name"
```

Expected output: tables in `bronze`, `silver`, `gold`, and `ref` schemas.

---

## 6. Step 4: Migrate Data

### 6.1 Strategy: bcp bulk export/import

`bcp` (bulk copy program) is the fastest way to move data between two SQL Server
instances. It exports table data to flat files in native binary format, then
imports them on the destination. Both instances sit on the same VPC subnet
(`10.0.0.0/24`), so the VM can reach Cloud SQL directly over its private IP.

The migration runs **entirely on the VM** — it pulls data from Cloud SQL over
the network and writes locally:

```
VM (stoxx-sql)
  ├── bcp out → connects to Cloud SQL at <private-ip>:1433 → /tmp/bcp_export/*.dat
  └── bcp in  → connects to localhost:1433 ← /tmp/bcp_export/*.dat
```

### 6.2 Scripts

Two scripts were created in `db/`:

| Script | Purpose |
|--------|---------|
| `migrate_data.sh` | Loops over all 20 tables, exports each from source (Cloud SQL) and imports into destination (VM) |
| `verify_migration.sh` | Compares row counts between source and destination for all tables |

### 6.3 Copy scripts to VM

From **local machine**:

```bash
gcloud compute scp db/migration/migrate_data.sh db/migration/verify_migration.sh stoxx-sql:/tmp/ \
  --zone=europe-west1-b --tunnel-through-iap
```

### 6.4 Fix Windows line endings

Scripts authored on Windows have CRLF line endings. Bash on Linux cannot parse
`\r\n` — it treats `\r` as part of the command, producing errors like:

```
: invalid option name line 2: set: pipefail
```

**Fix:** Strip carriage returns with `sed` before running:

```bash
sed -i 's/\r$//' /tmp/migrate_data.sh /tmp/verify_migration.sh
```

| Component | Purpose |
|-----------|---------|
| `sed -i` | In-place edit (modifies the file directly) |
| `'s/\r$//'` | Regex: replace carriage return (`\r`) at end of line (`$`) with nothing |

### 6.5 Set environment variables

On the **VM**, set the connection credentials. These three values come from
Terraform and GCE metadata:

```bash
# Cloud SQL private IP — get from Terraform output
#   terraform -chdir=infra output sql_cloud_ip
export SOURCE_IP="<cloud-sql-private-ip>"

# Cloud SQL password — same db_password variable used in terraform.tfvars
export SOURCE_PWD="<cloud-sql-password>"

# VM SA password — stored in instance metadata, retrieve with:
#   gcloud compute instances describe stoxx-sql --zone=europe-west1-b \
#     --format="get(metadata.items[sa-password])"
export DEST_PWD="<vm-sa-password>"
```

| Variable | Source | Used by |
|----------|--------|---------|
| `SOURCE_IP` | `terraform output sql_cloud_ip` | bcp connects to Cloud SQL |
| `SOURCE_PWD` | `terraform.tfvars` → `db_password` | Cloud SQL admin password (user: `sqlserver`) |
| `DEST_PWD` | GCE instance metadata `sa-password` | VM SQL Server admin password (user: `sa`) |

Replace the `<...>` placeholders with actual values before running.

### 6.6 Run migration

```bash
bash /tmp/migrate_data.sh
```

The script iterates over 20 tables in dependency order:

**Bronze (11 tables):**
- `bronze.dim_country` — ISO country codes (reference data)
- `bronze.dim_index` — index metadata (reference data)
- `bronze.index_dim` — stock dimensions per index
- `bronze.trading_calendar` — exchange trading day calendar
- `bronze.signals_daily` — daily price/valuation signals
- `bronze.signals_quarterly` — quarterly financial signals
- `bronze.pulse` — intraday price snapshots
- `bronze.pulse_tickers` — active ticker rankings
- `bronze.eurostoxx50_ohlcv` — OHLCV prices (Euro Stoxx 50)
- `bronze.stoxxasia50_ohlcv` — OHLCV prices (Stoxx Asia 50)
- `bronze.stoxxusa50_ohlcv` — OHLCV prices (Stoxx USA 50)

**Silver (6 tables):**
- `silver.index_dim` — cleaned dimensions (SCD Type 2)
- `silver.signals_daily` — deduplicated daily signals
- `silver.signals_quarterly` — deduplicated quarterly signals
- `silver.eurostoxx50_ohlcv` — cleaned OHLCV (Euro Stoxx 50)
- `silver.stoxxasia50_ohlcv` — cleaned OHLCV (Stoxx Asia 50)
- `silver.stoxxusa50_ohlcv` — cleaned OHLCV (Stoxx USA 50)

**Gold (3 tables):**
- `gold.scores_daily` — computed daily scores and rankings
- `gold.scores_quarterly` — computed quarterly scores
- `gold.index_performance` — index-level time series

For each table, `bcp` runs two operations:

```bash
# Export: read from Cloud SQL, write to native-format file
bcp "schema.table" out /tmp/bcp_export/schema.table.dat \
  -S "$SOURCE_IP" -U sqlserver -P "$SOURCE_PWD" -d stoxx -n -Yu

# Import: read from file, write to local SQL Server
# -E preserves IDENTITY column values (keeps original IDs)
bcp "schema.table" in /tmp/bcp_export/schema.table.dat \
  -S 127.0.0.1 -U sa -P "$DEST_PWD" -d stoxx -n -E -Yu
```

| Flag | Purpose |
|------|---------|
| `out` / `in` | Export / import direction |
| `-S` | Server address (source IP or localhost) |
| `-U` / `-P` | Username / password |
| `-d stoxx` | Database name |
| `-n` | Native format — binary, fastest, preserves types exactly between same-version SQL Server instances |
| `-E` | Keep identity values — without this, the destination would auto-generate new IDs |
| `-Yu` | Trust server certificate — **not** `-C` (which is code page in `bcp`). `sqlcmd` uses `-C`, but `bcp` uses `-Yu` |

### 6.7 Verify migration

```bash
bash /tmp/verify_migration.sh
```

Runs `SELECT COUNT(*)` on every table against both source and destination, then
compares:

```
TABLE                                     SOURCE         DEST STATUS
-----                                     ------         ---- ------
bronze.dim_country                           212          212 OK
bronze.dim_index                               3            3 OK
...
```

### 6.8 First run results & issues

The first migration run revealed two problems:

#### Issue 1: bcp SSL certificate error (`-C` vs `-Yu`)

```
SQLState = 08001, NativeError = 4294967295
Error = [Microsoft][ODBC Driver 18 for SQL Server]SSL Provider:
  certificate verify failed: unable to get local issuer certificate
```

**Cause:** The script used `-C` to trust the server certificate, but `-C` in
`bcp` is the **code page** flag, not trust certificate. That's `sqlcmd`'s flag.
In `bcp`, the equivalent is `-Yu` (strict encryption with trust).

**Fix:** Replace `-C` with `-Yu` in all `bcp` commands. `sqlcmd` in the verify
script correctly uses `-C`.

This also explains why the first run showed 0 rows imported for most tables —
the exports silently failed on SSL, producing empty or corrupt `.dat` files.
Only `bronze.dim_country` worked because it had been imported from a previous
manual test.

#### Issue 2: OHLCV tables don't exist on VM (Msg 208)

```
bronze.eurostoxx50_ohlcv    50  Msg208,Level16,State1  MISMATCH
silver.eurostoxx50_ohlcv  66149  Msg208,Level16,State1  MISMATCH
```

**Cause:** The 6 OHLCV tables (`bronze.*_ohlcv`, `silver.*_ohlcv`) are **not**
created by the DDL schema scripts. They are created dynamically by
`utils/setup_index.py` when a new index is configured. The DDL scripts only
create the static tables.

**Fix:** Run `setup_index.py` on the VM to create the OHLCV tables before
importing, or create them manually with `sqlcmd`.

#### Issue 3: Most tables exported but imported 0 rows

Only `bronze.dim_country` (212 rows) imported successfully. All other tables
show 0 rows on the destination despite successful exports.

**Root cause:** Same as Issue 1 — the `-C` flag was silently ignored by `bcp`,
so SSL verification failed on every connection to Cloud SQL. The export files
were empty or never written.

**Fix:** Use `-Yu` flag. After fixing, re-run the migration script to
re-export and re-import all tables.

#### Issue 4: `set -o pipefail` causing false failures in script

The migration script used `| tail -1` to show only the last line of bcp output,
but with `set -o pipefail`, the pipeline inherited bcp's non-zero exit code
(bcp returns non-zero in some success cases), causing all tables to report as
failed even when they succeeded.

**Fix:** Capture bcp output into a variable with `|| true`, then grep for
"error" in the output to detect real failures instead of relying on exit codes.

#### Issue 5: `silver.index_dim` import — QUOTED_IDENTIFIER

```
SQLState = 37000, NativeError = 1934
Error = INSERT failed because the following SET options have incorrect
settings: 'QUOTED_IDENTIFIER'
```

**Cause:** `silver.index_dim` has a filtered unique index
(`WHERE is_current = 1`). SQL Server requires `SET QUOTED_IDENTIFIER ON` for
INSERTs into tables with filtered indexes. `bcp` does not set this by default.

**Fix:** Drop the filtered index before import, import, then recreate:

```bash
# Drop
sqlcmd -S localhost -U sa -P "$DEST_PWD" -C -d stoxx -Q \
  "IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'UX_silver_index_dim_current') \
   DROP INDEX UX_silver_index_dim_current ON silver.index_dim"

# Import
bcp "silver.index_dim" in /tmp/bcp_export/silver.index_dim.dat \
  -S 127.0.0.1 -U sa -P "$DEST_PWD" -d stoxx -n -E -Yu

# Recreate (must SET QUOTED_IDENTIFIER ON first)
sqlcmd -S localhost -U sa -P "$DEST_PWD" -C -d stoxx -Q \
  "SET QUOTED_IDENTIFIER ON; \
   CREATE UNIQUE INDEX UX_silver_index_dim_current \
   ON silver.index_dim (_index, symbol) WHERE is_current = 1"
```

### 6.9 Final verification

After resolving all issues, the verification script confirmed all 20 tables
match between source and destination:

```
TABLE                                     SOURCE         DEST STATUS
-----                                     ------         ---- ------
bronze.dim_country                           212          212 OK
bronze.dim_index                               3            3 OK
bronze.index_dim                             150          150 OK
bronze.trading_calendar                    29251        29251 OK
bronze.signals_daily                         150          150 OK
bronze.signals_quarterly                     150          150 OK
bronze.pulse                                  30           30 OK
bronze.pulse_tickers                          30           30 OK
bronze.eurostoxx50_ohlcv                      50           50 OK
bronze.stoxxasia50_ohlcv                      50           50 OK
bronze.stoxxusa50_ohlcv                       50           50 OK
silver.index_dim                             247          247 OK
silver.signals_daily                         835          835 OK
silver.signals_quarterly                     156          156 OK
silver.eurostoxx50_ohlcv                   66149        66149 OK
silver.stoxxasia50_ohlcv                   63837        63837 OK
silver.stoxxusa50_ohlcv                    64950        64950 OK
gold.scores_daily                            591          591 OK
gold.scores_quarterly                        150          150 OK
gold.index_performance                      3969         3969 OK

All tables match. Migration verified.
```

Total rows migrated: **230,462** across 20 tables (bronze, silver, gold).

---

## 7. Step 5: Flip the Switch

### 7.1 Apply the toggle

```bash
terraform -chdir=infra apply -var="use_sql_vm=true"
```

This single variable change updates all 3 Cloud Run services (dashboard,
pipeline, setup):

| Env var | Before (`false`) | After (`true`) |
|---------|-------------------|----------------|
| `DB_HOST` | Cloud SQL private IP | VM internal IP |
| `DB_USER` | `sqlserver` | `sa` |
| `DB_PASSWORD` | unchanged | unchanged |

The Terraform plan should show **only** Cloud Run service revisions being
updated — no infrastructure changes.

### 7.2 How it works

In `run.tf`, the locals resolve based on the toggle:

```hcl
locals {
  sql_ip   = var.use_sql_vm ? local.sql_vm_ip : local.sql_cloud_ip
  sql_user = var.use_sql_vm ? "sa" : "sqlserver"
}
```

All Cloud Run `env` blocks reference `local.sql_ip` and `local.sql_user`,
so the switch is atomic — all services flip at once on `terraform apply`.

### 7.3 Verify pipeline writes to VM

After flipping the switch, trigger a full pipeline run from Airflow and verify fresh data lands on the VM.

#### 1. Check Cloud Run logs (exit code 0 = success)

```bash
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=stoxx-pipeline" --limit=30 --format="value(textPayload)"
```

Look for `Container called exit(0)`. Datadog trace timeout errors (`failed to send, dropping N traces`) are harmless — the DD agent IP isn't reachable from Cloud Run.

#### 2. SSH into the SQL VM and check latest timestamps

```bash
gcloud compute ssh stoxx-sql --zone=europe-west1-b --tunnel-through-iap
```

Then run (note: `sqlcmd` requires `-d stoxx` to select the database):

```bash
sqlcmd -S localhost -U sa -P '<password>' -C -d stoxx -Q "
SELECT 'bronze.signals_daily' AS tbl, MAX(_ingested_at) AS latest
FROM bronze.signals_daily
UNION ALL
SELECT 'gold.scores_daily', MAX(_scored_at) FROM gold.scores_daily
UNION ALL
SELECT 'gold.index_performance', MAX(_computed_at) FROM gold.index_performance
"
```

Expected output — all timestamps from today:

```
tbl                    latest
---------------------- --------------------------------------
bronze.signals_daily              2026-03-07 14:33:24.6647659
gold.scores_daily                 2026-03-07 14:34:48.4268513
gold.index_performance            2026-03-07 14:36:00.3271581
```

#### 3. Check the dashboard

Open the dashboard URL — data should reflect the latest pipeline run (updated prices, scores, charts).

#### Gotchas

- **PuTTY fatal error on Windows:** Don't pass long `sqlcmd` commands inline via `gcloud compute ssh ... -- "sqlcmd ..."`. SSH in interactively first, then run the query.
- **`sqlcmd: command not found`:** You're on the wrong VM (e.g., Airflow VM). SSH into `stoxx-sql`, not `stoxx-airflow`.
- **`Invalid object name 'bronze.signals_daily'`:** Missing `-d stoxx` flag — sqlcmd defaults to the `master` database.

### 7.4 Rollback (if needed)

If something goes wrong, flip back immediately:

```bash
terraform -chdir=infra apply -var="use_sql_vm=false"
```

This reverts all services to Cloud SQL within seconds.

---

## 8. Step 5b: Firewall — Local PC Access

After flipping the switch, the Airflow UI became unreachable from the browser. Root cause: the firewall rule `stoxx-allow-airflow` only allowed Google IAP range (`35.235.240.0/20`), not the admin's public IP.

### 8.1 Problem

GCP firewall rules are declarative in Terraform. Running `terraform apply` resets any manual firewall changes back to what's defined in code. The original rule only allowed IAP:

```hcl
source_ranges = ["35.235.240.0/20"]  # IAP only — no browser access
```

### 8.2 Solution: `admin_ip` Terraform variable

Added a variable so the admin's public IP is included in the firewall rule without being committed to GitHub.

**`infra/variables.tf`** — new variable:
```hcl
variable "admin_ip" {
  description = "Admin public IPv4 for Airflow UI access (e.g. 1.2.3.4)"
  type        = string
  default     = ""
}
```

**`infra/network.tf`** — dynamic source ranges:
```hcl
source_ranges = compact(concat(
  ["35.235.240.0/20"],
  var.admin_ip != "" ? ["${var.admin_ip}/32"] : []
))
```

**`infra/terraform.tfvars`** (gitignored — never committed):
```
admin_ip = "YOUR_PUBLIC_IP"
```

### 8.3 How to set up

1. **Get your public IPv4:**
   ```bash
   curl -4 ifconfig.me
   ```

2. **Add to `infra/terraform.tfvars`:**
   ```
   admin_ip = "YOUR_IP"
   ```

3. **Apply:**
   ```bash
   terraform -chdir=infra apply
   ```

4. **Access Airflow UI:**
   Open `http://<VM_IP>:8080` in your browser (credentials: `admin` / `admin`).

### 8.4 If your IP changes

ISPs periodically rotate public IPs. If Airflow UI stops loading:

```bash
curl -4 ifconfig.me              # get new IP
# update infra/terraform.tfvars with new IP
terraform -chdir=infra apply     # apply firewall change
```

### 8.5 Gotchas

- **IPv4 only.** GCP firewall rules can't mix IPv4 and IPv6 in the same rule. The `admin_ip` variable expects an IPv4 address.
- **`terraform.tfvars` is gitignored.** The IP never reaches GitHub. If you clone the repo on a new machine, you'll need to recreate this file.
- **IAP tunnel as fallback.** If you can't update the firewall (e.g., no Terraform access), use an IAP tunnel instead:
  ```bash
  gcloud compute start-iap-tunnel stoxx-airflow 8080 \
    --local-host-port=localhost:8080 --zone=europe-west1-b
  ```
  Then open `http://localhost:8080`.

---

## 9. Step 6: Decommission Cloud SQL

With the VM verified (pipeline writing fresh data, dashboard live), Cloud SQL can be removed.

### 9.1 Phase 1: Disable deletion protection

Cloud SQL has `deletion_protection = true` — Terraform refuses to destroy it. First flip it:

**`infra/sql.tf`** — change line 42:
```hcl
deletion_protection = false   # was: true
```

```bash
terraform -chdir=infra apply
```

This only updates the protection flag — nothing is destroyed yet.

### 9.2 Phase 2: Remove all Cloud SQL references

| File | What to remove / change |
|------|------------------------|
| `infra/sql.tf` | **Delete entire file** |
| `infra/network.tf` | Remove `google_compute_global_address.private_ip_range` and `google_service_networking_connection.private_vpc` (VPC peering for Cloud SQL private IP) |
| `infra/iam.tf` | Remove `google_project_iam_member.pipeline_sql` and `google_project_iam_member.dashboard_sql` (`roles/cloudsql.client` bindings) |
| `infra/run.tf` | Remove `sql_cloud_ip` local, remove `use_sql_vm` conditional — hardcode `sql_ip` to VM IP and `sql_user` to `"sa"`. Remove `google_sql_database.stoxx` from all `depends_on` |
| `infra/variables.tf` | Remove `db_tier` and `use_sql_vm` variables |
| `infra/outputs.tf` | Remove `sql_cloud_ip` and `sql_private_ip` outputs (keep `sql_vm_ip`) |

**`infra/run.tf` locals — before:**
```hcl
locals {
  registry     = "..."
  sql_vm_ip    = google_compute_instance.sql.network_interface[0].network_ip
  sql_cloud_ip = google_sql_database_instance.main.private_ip_address
  sql_ip       = var.use_sql_vm ? local.sql_vm_ip : local.sql_cloud_ip
  sql_user     = var.use_sql_vm ? "sa" : "sqlserver"
}
```

**After:**
```hcl
locals {
  registry = "..."
  sql_ip   = google_compute_instance.sql.network_interface[0].network_ip
  sql_user = "sa"
}
```

### 9.3 Apply Phase 2

```bash
terraform -chdir=infra apply
```

Terraform plan should show destruction of:
- `google_sql_database.stoxx`
- `google_sql_database_instance.main`
- `google_compute_global_address.private_ip_range`
- `google_service_networking_connection.private_vpc`
- `google_project_iam_member.pipeline_sql`
- `google_project_iam_member.dashboard_sql`

Type `yes`. Cloud SQL deletion takes 5-10 minutes.

### 9.4 Issue: VPC peering stuck

After Cloud SQL is destroyed, the VPC peering deletion may fail with:

```
Error: Failed to delete connection; Producer services (e.g. CloudSQL,
Cloud Memstore, etc.) are still using this connection.
```

**Cause:** GCP hasn't fully cleaned up the Cloud SQL instance yet (takes up to 15 minutes internally even after `terraform apply` reports success).

**Fix:** Remove the two resources from Terraform state so it stops trying to delete them:

```bash
cd infra
terraform state rm google_service_networking_connection.private_vpc
terraform state rm google_compute_global_address.private_ip_range
```

Then re-apply:

```bash
terraform apply
```

GCP will clean up the peering and IP range automatically once the Cloud SQL instance fully disappears from the backend. These orphaned GCP resources cost nothing and will be garbage-collected.

### 9.5 Verify

After apply completes with no errors:

```bash
# Should show no Cloud SQL instances
gcloud sql instances list

# Dashboard still loads
# Pipeline still writes (check next scheduled run)

# Terraform state is clean
terraform -chdir=infra state list | grep -i sql
# Should only show: google_compute_instance.sql (the VM)
```

### 9.6 Cost savings

| Resource removed | Monthly cost saved |
|------------------|--------------------|
| Cloud SQL (db-custom-1-3840, PD_SSD) | ~$50-60 |
| VPC peering / private IP range | $0 (no direct cost) |
| **Total** | **~$50-60/month** |

The VM SQL Server (e2-medium, 20 GB disk) costs ~$27/month — net savings of ~$25-35/month.

---

## 10. Troubleshooting

### VM can't download packages (curl timeout)

**Cause:** VM has no public IP and no Cloud NAT configured.
**Fix:** Add Cloud NAT to the VPC (see Section 3.1). Apply, then re-run the startup script.

### GPG key error / "repository is not signed"

**Cause:** Ubuntu 22.04 requires `signed-by` in apt source files. Microsoft's `.list` URLs don't include it.
**Fix:** Manually write the `.list` files with `signed-by=/usr/share/keyrings/microsoft-prod.gpg` (see Section 4.3).

### "Unit mssql-server.service could not be found"

**Cause:** SQL Server was never installed (startup script failed on first boot).
**Fix:** Install manually after fixing internet access (see Sections 4.2–4.5).

### Startup script doesn't re-run on reboot

GCE runs `startup-script` on **every boot**, not just the first. The script has a marker file (`/var/lib/sql-setup-done`) — on subsequent boots it just starts the service and exits.

To force a full re-install: `sudo rm /var/lib/sql-setup-done` then reboot or re-run manually.
