# Terraform Infrastructure Setup

All GCP infrastructure is defined in the `infra/` directory using Terraform with a GCS remote backend.
Changes are applied manually from a local machine — there is no Terraform in CI/CD.

## Architecture

```
GCP Project: <PROJECT_ID> (europe-west1)
│
├── VPC: stoxx-vpc (10.0.0.0/24)
│   ├── Private Service Access ──► Cloud SQL peering
│   ├── Firewall: SSH (22), Airflow UI (8080), IAP tunnel
│   │
│   ├── Cloud SQL: stoxx-sql
│   │   └── SQL Server 2022 Express (private IP only)
│   │       └── Database: stoxx
│   │
│   ├── Cloud Run: stoxx-dashboard
│   │   └── Blazor web app (public, no auth)
│   │
│   ├── Cloud Run Job: stoxx-pipeline
│   │   └── Python pipeline (triggered by Airflow)
│   │
│   ├── Cloud Run Job: stoxx-setup
│   │   └── DDL + seed data (run once)
│   │
│   └── GCE VM: stoxx-airflow
│       └── e2-medium, COS, 4 Docker containers
│
├── Artifact Registry: stoxx (Docker images)
│
├── Secret Manager: stoxx-db-password
│
└── Service Accounts
    ├── stoxx-pipeline   (Cloud SQL + Secrets)
    ├── stoxx-dashboard  (Cloud SQL + Secrets)
    ├── stoxx-airflow    (Cloud Run invoker)
    └── stoxx-ci         (Registry + Cloud Run deploy)
```

## Files

| File | Resources | Description |
|------|-----------|-------------|
| `main.tf` | provider, backend | Google provider ~6.0, GCS backend at `stoxx-tf-state` |
| `variables.tf` | 5 variables | project_id, region, zone, db_password, db_tier |
| `network.tf` | VPC, subnet, firewall | stoxx-vpc with private service access for Cloud SQL |
| `sql.tf` | Cloud SQL instance + DB | SQL Server 2022 Express, private IP, 10 GB SSD |
| `secrets.tf` | Secret Manager | Stores `db_password` |
| `registry.tf` | Artifact Registry | Docker image repo `stoxx` |
| `run.tf` | 3 Cloud Run resources | Dashboard (service), Pipeline (job), Setup (job) |
| `compute.tf` | GCE instance | Airflow VM with startup script |
| `iam.tf` | 4 service accounts + bindings | Pipeline, Dashboard, Airflow, CI |
| `ci.tf` | CI service account + IAM | GitHub Actions: push images + deploy Cloud Run |
| `outputs.tf` | 6 outputs | dashboard_url, sql_private_ip, registry, airflow_ip, etc. |

## Prerequisites

1. **Terraform >= 1.5** installed locally
2. **gcloud CLI** authenticated with project owner or editor role:
   ```bash
   gcloud auth application-default login
   ```
3. **GCS backend bucket** created (one-time, outside Terraform):
   ```bash
   gcloud storage buckets create gs://stoxx-tf-state \
     --project=<PROJECT_ID> --location=europe-west1
   ```
4. **APIs enabled** (Terraform will prompt if missing, but you can enable upfront):
   ```bash
   gcloud services enable \
     compute.googleapis.com \
     sqladmin.googleapis.com \
     run.googleapis.com \
     artifactregistry.googleapis.com \
     secretmanager.googleapis.com \
     servicenetworking.googleapis.com \
     --project=<PROJECT_ID>
   ```

## Variables

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `project_id` | string | — | `<PROJECT_ID>` |
| `region` | string | `europe-west1` | GCP region for all resources |
| `zone` | string | `europe-west1-b` | Zone for the Airflow VM |
| `db_password` | string (sensitive) | — | Cloud SQL `sqlserver` admin password |
| `db_tier` | string | `db-custom-1-3840` | Cloud SQL machine type (1 vCPU, 3.75 GB) |

Set via `terraform.tfvars` (git-ignored) or `-var` flags:

```bash
# infra/terraform.tfvars
project_id  = "<PROJECT_ID>"
db_password = "your-password-here"
```

## Initial Deployment

### 1. Initialize Terraform

```bash
terraform -chdir=infra init
```

Downloads the Google provider and configures the GCS backend. Run once, or after adding new providers.

### 2. Plan all resources

```bash
terraform -chdir=infra plan
```

Shows what will be created. First run creates ~25 resources. Review the plan carefully — especially
Cloud SQL (takes 10-15 min) and VPC peering.

### 3. Apply

```bash
terraform -chdir=infra apply
```

Type `yes` when prompted. Full initial apply takes ~15-20 minutes (Cloud SQL is the bottleneck).

### 4. Verify outputs

```bash
terraform -chdir=infra output
```

Expected:
```
airflow_ip    = "35.x.x.x"
dashboard_url = "https://stoxx-dashboard-xxxxx.a.run.app"
pipeline_job  = "stoxx-pipeline"
registry      = "europe-west1-docker.pkg.dev/<PROJECT_ID>/stoxx"
setup_job     = "stoxx-setup"
sql_private_ip = "10.x.x.x"
```

### 5. Run the setup job (one-time)

After first apply, run DDL + seed data:

```bash
gcloud run jobs execute stoxx-setup --region=europe-west1 --wait
```

This creates the database schema (bronze/silver/gold layers) and seeds index definitions.

## Common Operations

### Apply a single resource (targeted)

```bash
terraform -chdir=infra apply -target=google_compute_instance.airflow
```

Useful when you only changed the Airflow VM or startup script. Faster than full apply.

Other common targets:
```bash
# Cloud Run dashboard service
terraform -chdir=infra apply -target=google_cloud_run_v2_service.dashboard

# Cloud Run pipeline job
terraform -chdir=infra apply -target=google_cloud_run_v2_job.pipeline

# Firewall rules
terraform -chdir=infra apply -target=google_compute_firewall.allow_airflow_ui
```

### Check current state

```bash
terraform -chdir=infra show
```

Displays the full state of all managed resources.

### List all managed resources

```bash
terraform -chdir=infra state list
```

Expected: ~25 resources. Useful to verify what Terraform is tracking.

### Refresh state from GCP

```bash
terraform -chdir=infra refresh
```

Syncs Terraform state with actual GCP state. Use when resources were modified outside Terraform
(e.g., manual changes in the console).

### Import an existing resource

```bash
terraform -chdir=infra import google_compute_instance.airflow \
  projects/<PROJECT_ID>/zones/europe-west1-b/instances/stoxx-airflow
```

Use when a resource already exists in GCP but isn't in the Terraform state.

### Destroy a single resource

```bash
terraform -chdir=infra destroy -target=google_cloud_run_v2_job.setup
```

Removes a specific resource from GCP and the state. Use with caution.

## Service Accounts & IAM

| Service Account | Roles | Used By |
|----------------|-------|---------|
| `stoxx-pipeline` | `cloudsql.client`, Secret Accessor | Cloud Run pipeline + setup jobs |
| `stoxx-dashboard` | `cloudsql.client`, Secret Accessor | Cloud Run dashboard service |
| `stoxx-airflow` | `run.invoker`, `run.developer`, `logging.viewer` | Airflow VM (triggers Cloud Run jobs) |
| `stoxx-ci` | `artifactregistry.writer`, `run.developer`, act-as pipeline+dashboard | GitHub Actions CI/CD |

The VM's service account (`stoxx-airflow`) is what Airflow uses to authenticate with GCP when
calling `CloudRunExecuteJobOperator`. The `google_cloud_default` Airflow connection uses
`google.auth.default()`, which picks up the VM's service account credentials automatically.

## Networking

**VPC:** `stoxx-vpc` with a single subnet `stoxx-subnet` (10.0.0.0/24) in europe-west1.

**Cloud SQL private IP:** Cloud SQL has no public IP. All access goes through VPC peering
(`google_service_networking_connection`). Cloud Run connects via `vpc_access` with
`egress = "PRIVATE_RANGES_ONLY"` — only traffic to private IPs routes through the VPC.

**Firewall rules:**

| Rule | Ports | Source | Target |
|------|-------|--------|--------|
| `stoxx-allow-ssh` | TCP 22 | 0.0.0.0/0 | tag: `airflow` |
| `stoxx-allow-airflow` | TCP 8080 | 0.0.0.0/0 | tag: `airflow` |
| `stoxx-allow-iap` | TCP 22 | 35.235.240.0/20 | tag: `airflow` |

## Debugging

### Terraform state issues

**Check what Terraform thinks exists:**
```bash
terraform -chdir=infra state list
```
Expected: ~25 resources starting with `google_*`. If empty or missing resources, the state
file may be out of sync.

---

**Show details of a specific resource:**
```bash
terraform -chdir=infra state show google_sql_database_instance.main
```
Shows all attributes (IP, status, version, etc.) as Terraform knows them. Compare with the
GCP console to spot drift.

---

**Detect drift (plan with no changes):**
```bash
terraform -chdir=infra plan
```
Expected: `No changes. Your infrastructure matches the configuration.`

If it shows unexpected changes:
- `~ update in-place` = Terraform wants to modify a resource (check which attribute drifted)
- `- destroy` then `+ create` = resource will be recreated (dangerous for Cloud SQL — data loss!)
- `must be replaced` = same as above, happens when immutable attributes change

---

### Cloud SQL

**Check instance status:**
```bash
gcloud sql instances describe stoxx-sql --format="get(state)"
```
Expected: `RUNNABLE`

Other states: `PENDING_CREATE` (still provisioning, wait 10-15 min), `SUSPENDED`, `MAINTENANCE`.

---

**Test connectivity from Cloud Run:**
```bash
gcloud run jobs execute stoxx-setup --region=europe-west1 --wait
```
If it fails with connection timeout: VPC peering may be misconfigured. Check:
```bash
gcloud services vpc-peerings list --network=stoxx-vpc
```
Expected: one peering to `servicenetworking.googleapis.com`.

---

**Check Cloud SQL private IP:**
```bash
gcloud sql instances describe stoxx-sql \
  --format="get(ipAddresses[0].ipAddress)"
```
This should match `terraform output sql_private_ip`. If they differ, run `terraform refresh`.

---

### Cloud Run

**Check dashboard service status:**
```bash
gcloud run services describe stoxx-dashboard \
  --region=europe-west1 --format="get(status.url)"
```
Expected: returns the public URL. If the URL works but shows errors, check logs:
```bash
gcloud run services logs read stoxx-dashboard --region=europe-west1 --limit=20
```

---

**Check pipeline job status:**
```bash
gcloud run jobs describe stoxx-pipeline \
  --region=europe-west1 --format="get(latestCreatedExecution.name)"
```

---

**List recent pipeline executions:**
```bash
gcloud run jobs executions list --job=stoxx-pipeline --region=europe-west1 --limit=10
```
Shows status (`Succeeded` / `Failed`), start time, and duration. If all are `Failed`, check:
```bash
gcloud run jobs executions describe <EXECUTION-NAME> \
  --region=europe-west1
```

---

**View pipeline execution logs:**
```bash
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=stoxx-pipeline" \
  --limit=50 --format="value(textPayload)"
```
Shows the Python pipeline output. Look for:
- `Step X completed` = success
- `ConnectionError` / `Login timeout expired` = Cloud SQL connectivity issue
- `Permission denied` = service account missing roles

---

### Artifact Registry

**List images:**
```bash
gcloud artifacts docker images list \
  europe-west1-docker.pkg.dev/<PROJECT_ID>/stoxx
```
Expected: `pipeline` and `dashboard` images with tags.

If empty: CI hasn't pushed images yet, or the registry doesn't exist.

---

**Check who can push:**
```bash
gcloud artifacts repositories get-iam-policy stoxx \
  --location=europe-west1
```
The `stoxx-ci` service account needs `roles/artifactregistry.writer`.

---

### Firewall & Network

**List firewall rules:**
```bash
gcloud compute firewall-rules list --filter="network~stoxx-vpc"
```
Expected: 3 rules (`stoxx-allow-ssh`, `stoxx-allow-airflow`, `stoxx-allow-iap`).

---

**Test port 8080 reachability (from local PC):**
```bash
curl -s -o /dev/null -w "%{http_code}" http://<AIRFLOW-IP>:8080/health
```
Expected: `200`. If connection refused: check firewall rule exists and VM is running.
If timeout: the VM's network tag may not match the firewall target tag.

---

### Service Accounts

**List service accounts:**
```bash
gcloud iam service-accounts list --filter="email~stoxx"
```
Expected: 4 accounts (`stoxx-pipeline`, `stoxx-dashboard`, `stoxx-airflow`, `stoxx-ci`).

---

**Check roles for a service account:**
```bash
gcloud projects get-iam-policy <PROJECT_ID> \
  --flatten="bindings[].members" \
  --filter="bindings.members:stoxx-airflow" \
  --format="table(bindings.role)"
```
Expected for `stoxx-airflow`: `roles/run.invoker`, `roles/run.developer`, `roles/logging.viewer`.

If Airflow tasks fail with `403 Permission Denied`: the Airflow service account is missing
`roles/run.developer` or `roles/run.invoker`.

---

### Outputs

**View all outputs:**
```bash
terraform -chdir=infra output
```

**Get a specific output:**
```bash
terraform -chdir=infra output -raw dashboard_url
terraform -chdir=infra output -raw sql_private_ip
terraform -chdir=infra output -raw airflow_ip
```

If `airflow_ip` is stale (VM was stopped/started): run `terraform refresh` first.

## Common Issues

### "Error creating Instance: googleapi: Error 409: already exists"
The resource exists in GCP but not in Terraform state. Import it:
```bash
terraform -chdir=infra import google_compute_instance.airflow \
  projects/<PROJECT_ID>/zones/europe-west1-b/instances/stoxx-airflow
```

### "Error: Failed to get existing workspaces: querying Cloud Storage failed"
Backend bucket doesn't exist or you're not authenticated:
```bash
gcloud auth application-default login
gcloud storage buckets create gs://stoxx-tf-state \
  --project=<PROJECT_ID> --location=europe-west1
terraform -chdir=infra init
```

### Cloud SQL takes forever to create
SQL Server instances take 10-15 minutes to provision. This is normal. Don't cancel the apply.
If you do cancel mid-creation, the instance may be in a `PENDING_CREATE` state — wait for it
to finish, then run `terraform import` to bring it into state.

### "Error: Service Networking API must be enabled"
```bash
gcloud services enable servicenetworking.googleapis.com \
  --project=<PROJECT_ID>
```
Then re-run `terraform apply`.

### Cloud Run service shows "Revision not ready" after deploy
The container image may not exist in Artifact Registry yet. Push it first via CI or manually:
```bash
docker build -t europe-west1-docker.pkg.dev/<PROJECT_ID>/stoxx/dashboard:latest \
  -f docker/dashboard.Dockerfile .
docker push europe-west1-docker.pkg.dev/<PROJECT_ID>/stoxx/dashboard:latest
```

### "Terraform apply cancelled" / didn't have time to type yes
Terraform has a 10-second confirmation timeout. Just re-run the command. No state corruption
occurs if you cancel during the prompt.

### State lock error: "Error locking state"
Someone else (or a previous crashed run) holds the lock:
```bash
terraform -chdir=infra force-unlock <LOCK-ID>
```
The lock ID is shown in the error message. Only force-unlock if you're sure no other apply is running.

### VPC peering: "Cannot modify allocated ranges in CreateConnection"
This happens when re-creating the private service access. The peering range is immutable once
established. If you need to change it, delete the peering first:
```bash
gcloud services vpc-peerings delete --network=stoxx-vpc \
  --service=servicenetworking.googleapis.com
```
Then re-apply. **Warning:** this temporarily breaks Cloud SQL connectivity.

### Windows line endings in startup script
If the Airflow VM shows `env: 'bash\r': No such file or directory`, the startup script
has `\r\n` endings. Fix:
```bash
sed -i 's/\r$//' infra/scripts/airflow-startup.sh
terraform -chdir=infra apply -target=google_compute_instance.airflow
```
Then stop/start the VM for the new script to take effect.

## Costs

| Resource | Monthly Cost |
|----------|-------------|
| Cloud SQL Express (db-custom-1-3840, 10 GB SSD) | ~$55 |
| GCE e2-medium VM (continuous) | ~$25 |
| 20 GB pd-balanced disk (Airflow VM) | ~$2 |
| Cloud Run dashboard (0-2 instances) | ~$0-5 |
| Cloud Run pipeline jobs (per-execution) | ~$1-3 |
| Artifact Registry (storage) | < $1 |
| Secret Manager (1 secret, low access) | < $1 |
| **Total** | **~$85-90/month** |

Cloud SQL is the largest cost. The Express edition is the cheapest SQL Server option.
`db-custom-1-3840` is the minimum tier for SQL Server (cannot go lower).
