# Airflow Production Setup

Airflow runs on a dedicated GCE VM (`stoxx-airflow`) using Container-Optimized OS (COS).
It orchestrates the STOXX pipeline by triggering Cloud Run jobs on a schedule.

## Architecture

```
GCE VM: stoxx-airflow (e2-medium, COS)
│
├── Docker network: airflow-net
│   ├── airflow-postgres   (postgres:16-alpine)     ← metadata DB
│   ├── airflow-webserver  (airflow:2.10.5)          ← UI on port 8080
│   ├── airflow-scheduler  (airflow:2.10.5)          ← LocalExecutor
│   └── airflow-triggerer  (airflow:2.10.5)          ← deferred tasks
│
├── /home/airflow/pgdata   ← PostgreSQL data (persists across reboots)
├── /home/airflow/dags     ← DAG files (mounted into all Airflow containers)
└── /home/airflow/logs     ← task logs
```

**Executor:** `LocalExecutor` with PostgreSQL — runs tasks in parallel (unlike
`SequentialExecutor` which processes one task at a time).

**Deployment mode:** Multi-container — each Airflow component (webserver, scheduler,
triggerer) runs as a separate Docker container, sharing a PostgreSQL metadata DB.
This is the standard production deployment pattern from the
[Airflow Docker docs](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html),
adapted for COS (plain `docker run` instead of `docker-compose`).

**Not `airflow standalone`** as it hardcodes `SequentialExecutor`. With 3 DAGs
(pulse every minute, tickers hourly, daily 3x/day), sequential execution causes
the pulse DAG to monopolize the executor and block the others.

## DAGs

| DAG | Schedule | Steps | Description |
|-----|----------|-------|-------------|
| `stoxx_daily` | `0 9,17,22 * * 1-5` | Full pipeline | 3x/day after each region closes |
| `stoxx_tickers` | `0 0-21 * * 1-5` | 10 → 11 | Hourly active ticker discovery |
| `stoxx_pulse` | `*/5 0-21 * * 1-5` | 12 → 13 | Pulse snapshots every 5 min |

All DAGs use `CloudRunExecuteJobOperator` to trigger the `stoxx-pipeline` Cloud Run job
with `--step N` arguments. The actual compute happens on Cloud Run, not on the Airflow VM.

## Terraform

The VM is managed by `infra/compute.tf`. The startup script at `infra/scripts/airflow-startup.sh`
is embedded into the VM metadata via:

```hcl
metadata = {
  startup-script = file("${path.module}/scripts/airflow-startup.sh")
}
```

The startup script is **idempotent** — it runs on every boot and skips containers that are
already running. It handles: directory creation, image pulls, PostgreSQL startup,
DB migration, admin user creation, GCP connection setup, and launching all Airflow components.

## Initial Deployment

### 1. Apply Terraform

```bash
terraform -chdir=infra apply -target=google_compute_instance.airflow
```

This creates (or updates) the VM. On first boot, the startup script pulls images and
initializes everything.

### 2. Get the VM IP

```bash
gcloud compute instances describe stoxx-airflow \
  --zone=europe-west1-b \
  --format="get(networkInterfaces[0].accessConfigs[0].natIP)"
```

The VM uses an ephemeral public IP that changes on every stop/start.

### 3. SSH into the VM

```bash
gcloud compute ssh stoxx-airflow --zone=europe-west1-b
```

### 4. Verify containers are running

```bash
docker ps
```

Expected output: 4 containers (`airflow-postgres`, `airflow-webserver`, `airflow-scheduler`, `airflow-triggerer`).

### 5. Deploy DAG files

DAG files must be copied to `/home/airflow/dags/` on the VM. COS has a restricted
filesystem, so use `sudo tee`:

```bash
sudo tee /home/airflow/dags/stoxx_daily.py << 'EOF'
from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from datetime import datetime

default_args = {
    "project_id": "<PROJECT-ID>",
    "region": "europe-west1",
    "job_name": "stoxx-pipeline",
    "deferrable": False,
}

with DAG(
    "stoxx_daily",
    description="Full STOXX pipeline: 3 staggered runs after each region closes",
    schedule="0 9,17,22 * * 1-5",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["stoxx"],
) as dag:
    run_pipeline = CloudRunExecuteJobOperator(
        task_id="run_pipeline",
        **default_args,
    )
EOF
```

```bash
sudo tee /home/airflow/dags/stoxx_tickers.py << 'EOF'
from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from datetime import datetime

with DAG(
    "stoxx_tickers",
    description="Hourly refresh of most active tickers per index",
    schedule="0 0-21 * * 1-5",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["stoxx"],
) as dag:
    fetch_tickers = CloudRunExecuteJobOperator(
        task_id="fetch_tickers",
        project_id="stoxx-index-intelligence",
        region="europe-west1",
        job_name="stoxx-pipeline",
        overrides={"container_overrides": [{"args": ["--step", "10"]}]},
        deferrable=False,
    )
    load_tickers = CloudRunExecuteJobOperator(
        task_id="load_tickers",
        project_id="stoxx-index-intelligence",
        region="europe-west1",
        job_name="stoxx-pipeline",
        overrides={"container_overrides": [{"args": ["--step", "11"]}]},
        deferrable=False,
    )
    fetch_tickers >> load_tickers
EOF
```

```bash
sudo tee /home/airflow/dags/stoxx_pulse.py << 'EOF'
from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from datetime import datetime

with DAG(
    "stoxx_pulse",
    description="Real-time pulse snapshots for live dashboard",
    schedule="* 0-21 * * 1-5",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["stoxx"],
) as dag:
    fetch_pulse = CloudRunExecuteJobOperator(
        task_id="fetch_pulse",
        project_id="stoxx-index-intelligence",
        region="europe-west1",
        job_name="stoxx-pipeline",
        overrides={"container_overrides": [{"args": ["--step", "12"]}]},
        deferrable=False,
    )
    load_pulse = CloudRunExecuteJobOperator(
        task_id="load_pulse",
        project_id="stoxx-index-intelligence",
        region="europe-west1",
        job_name="stoxx-pipeline",
        overrides={"container_overrides": [{"args": ["--step", "13"]}]},
        deferrable=False,
    )
    fetch_pulse >> load_pulse
EOF
```

Fix ownership so the Airflow containers (UID 50000) can read them:

```bash
sudo chown -R 50000:0 /home/airflow/dags/
```

The scheduler picks up new/changed DAGs within ~30 seconds.

### 6. Access the Airflow UI

Open `http://<VM-IP>:8080` in your browser.

- **Username:** `admin`
- **Password:** `admin`

### 7. Unpause DAGs

DAGs start paused by default. Toggle the switch next to each DAG name to unpause.

## Updating DAG Schedules

Edit the file directly on the VM:

```bash
# Example: change pulse from every 5 min to every minute
sudo sed -i 's|*/5 0-21|* 0-21|' /home/airflow/dags/stoxx_pulse.py
```

The scheduler detects file changes automatically.

## Updating the Startup Script

After editing `infra/scripts/airflow-startup.sh` locally, push to VM metadata:

```bash
terraform -chdir=infra apply -target=google_compute_instance.airflow
```

This updates metadata without restarting the VM. The new script takes effect on next reboot.

## Debugging

### From the VM (SSH)

**Connect to the VM:**
```bash
gcloud compute ssh stoxx-airflow --zone=europe-west1-b
```
> First connection may prompt for a host key — type `yes`. If the terminal hangs
> on Windows (PuTTY host key prompt), close and retry.

---

**Check containers are running:**
```bash
docker ps
```
Expected: 4 containers with status `Up`:
```
NAMES               IMAGE                              STATUS
airflow-webserver   apache/airflow:2.10.5-python3.12   Up 5 minutes
airflow-scheduler   apache/airflow:2.10.5-python3.12   Up 5 minutes
airflow-triggerer   apache/airflow:2.10.5-python3.12   Up 5 minutes
airflow-postgres    postgres:16-alpine                 Up 6 minutes
```
If fewer than 4: check which is missing and look at its logs (see below).
Restart a stopped container: `docker restart airflow-scheduler` (replace with the missing name).
If you see a `klt--*` container: that's the old konlet container — kill it:
```bash
docker rm -f $(docker ps -q --filter "name=klt--")
```

---

**View container logs:**
```bash
docker logs airflow-scheduler --tail 50
docker logs airflow-webserver --tail 50
docker logs airflow-triggerer --tail 50
docker logs airflow-postgres --tail 50
```
What to look for:
- **scheduler:**
  - `Launched DagFileProcessorManager` = healthy
  - `No module named 'airflow.providers.google'` = provider not installed (shouldn't happen with official image)
- **webserver:**
  - `[INFO] Starting gunicorn` + `Workers: 4 sync` = healthy
  - `Already running on PID` = stale PID file (see Common Issues below)
  - `No response from gunicorn master within 120 seconds` = webserver crashed during startup
- **triggerer:**
  - `0 triggers currently running` = healthy (normal when no deferred tasks)
- **postgres:**
  - `database system is ready to accept connections` = healthy
  - `FATAL: password authentication failed` = wrong credentials in Airflow env vars

To follow logs in real-time: `docker logs -f airflow-scheduler`

---

**Check startup script execution:**
```bash
sudo journalctl -u google-startup-scripts.service -b --no-pager
```
What to look for:
- `Found startup-script in metadata` = script was detected.
- `PostgreSQL is ready` = DB initialized successfully.
- `User "admin" created` or `admin already exists` = admin user OK.
- `Airflow startup complete` = all containers launched.

Typical errors:
- `No startup scripts to run` = metadata not updated. Run `terraform apply` and stop/start VM.
- `env: 'bash\r': No such file or directory` = Windows line endings. Fix with `sed -i 's/\r$//' infra/scripts/airflow-startup.sh` then re-apply Terraform.
- `Script failed with error: exit status 125` = a `docker run` failed. Look for `bind: address already in use` (port conflict) or `Conflict. The container name ... is already in use`.

---

**Verify executor type:**
```bash
docker exec airflow-scheduler airflow config get-value core executor
```
Expected: `LocalExecutor`

If it returns `SequentialExecutor`: the env var `AIRFLOW__CORE__EXECUTOR` is not set
on the scheduler container, or you're accidentally running `airflow standalone`.

---

**Verify DB connection:**
```bash
docker exec airflow-scheduler airflow db check
```
Expected: `Connection successful.`

If it fails: PostgreSQL container may be down or the connection string is wrong.
Check: `docker logs airflow-postgres --tail 10`

---

**Check PostgreSQL health:**
```bash
docker exec airflow-postgres pg_isready -U airflow
```
Expected: `accepting connections`

If it returns `no response`: PostgreSQL is starting up or crashed. Check its logs.

---

**List DAGs:**
```bash
docker exec airflow-scheduler airflow dags list
```
Expected: 3 DAGs listed (`stoxx_daily`, `stoxx_pulse`, `stoxx_tickers`).

If empty or missing DAGs:
1. Check files exist: `ls -la /home/airflow/dags/`
2. Check ownership: `ls -ln /home/airflow/dags/` — should show UID `50000`, not `root`
3. Fix ownership if needed: `sudo chown -R 50000:0 /home/airflow/dags/`
4. Check for Python syntax errors: `docker exec airflow-scheduler python /opt/airflow/dags/stoxx_pulse.py`

---

**List connections:**
```bash
docker exec airflow-scheduler airflow connections list
```
Expected: `google_cloud_default` with conn_type `google_cloud_platform` in the list.

If missing: tasks will fail with `The conn_id 'google_cloud_default' isn't defined`. Add it:
```bash
docker exec airflow-scheduler airflow connections add \
  google_cloud_default --conn-type google_cloud_platform
```

---

**Test a task manually (without scheduling):**
```bash
docker exec airflow-scheduler airflow tasks test stoxx_pulse fetch_pulse 2026-03-02
```
Expected output:
```
Retrieving connection 'google_cloud_default'
Getting connection using `google.auth.default()`...
Marking task as SUCCESS
```
This runs the task immediately and shows full output. Useful to diagnose failures
without waiting for the scheduler.

---

**Check directory ownership:**
```bash
ls -ln /home/airflow/
```
Expected:
```
drwxr-xr-x  50000 0     dags
drwxr-xr-x  50000 0     logs
drwx------  999   999   pgdata
```
If `dags` or `logs` are owned by `root` (UID 0): containers can't write to them.
Fix: `sudo chown -R 50000:0 /home/airflow/dags /home/airflow/logs`

If `pgdata` is owned by `root`: PostgreSQL can't start.
Fix: `sudo chown -R 999:999 /home/airflow/pgdata`

---

**Restart a container:**
```bash
docker restart airflow-scheduler
```
Use when the scheduler is unresponsive or you've changed env vars. Applies to
any of the 4 containers. The `--restart unless-stopped` policy means Docker will
also auto-restart crashed containers.

---

**Check disk usage:**
```bash
df -h            # host disk (20 GB pd-balanced)
docker system df # Docker images, containers, volumes
```
If disk is full: clean unused Docker images with `docker image prune -f`.
The 20 GB disk should be sufficient — Airflow images are ~1.5 GB, PostgreSQL ~80 MB.

---

### From your local PC

**Get the current VM IP:**
```bash
gcloud compute instances describe stoxx-airflow \
  --zone=europe-west1-b \
  --format="get(networkInterfaces[0].accessConfigs[0].natIP)"
```
Returns the public IP address. This changes on every VM stop/start (ephemeral IP).
Use this IP to access the Airflow UI at `http://<IP>:8080`.

---

**Check VM status:**
```bash
gcloud compute instances describe stoxx-airflow \
  --zone=europe-west1-b \
  --format="get(status)"
```
Expected: `RUNNING`. Other values: `STOPPED`, `STAGING` (booting), `TERMINATED`.

---

**Stop and start VM (triggers startup script on boot):**
```bash
gcloud compute instances stop stoxx-airflow --zone=europe-west1-b
gcloud compute instances start stoxx-airflow --zone=europe-west1-b
```
Takes ~60 seconds. The IP changes after restart. Allow 2-3 minutes for all
containers to be ready (image pulls are cached after first boot).

---

**Apply Terraform changes to the VM only:**
```bash
terraform -chdir=infra apply -target=google_compute_instance.airflow
```
Updates VM metadata (startup script) without restarting. The new script takes
effect on the next reboot. If Terraform reports `No changes`: the local script
matches what's already in metadata.

---

**View boot logs without SSH:**
```bash
gcloud compute instances get-serial-port-output stoxx-airflow \
  --zone=europe-west1-b | tail -100
```
Useful when SSH is not accessible (e.g., container using all resources, or
networking issue). Shows kernel boot messages and startup script output.

---

**Check firewall rules:**
```bash
gcloud compute firewall-rules list --filter="name~airflow"
```
Expected: `stoxx-allow-airflow` allowing TCP port 8080 from `0.0.0.0/0`.

If the Airflow UI is not accessible: verify the rule exists and targets the
`airflow` network tag.

---

**View Cloud Run job executions (triggered by Airflow):**
```bash
gcloud run jobs executions list --job=stoxx-pipeline --region=europe-west1 --limit=10
```
Shows recent pipeline executions with status (`Succeeded` / `Failed`).
If Airflow tasks succeed but the pipeline output is wrong, check these logs:
```bash
gcloud run jobs executions describe <EXECUTION-ID> \
  --job=stoxx-pipeline --region=europe-west1
```

## Common Issues

### "No startup scripts to run"
The startup script has Windows line endings (`\r\n`). Fix locally with:
```bash
sed -i 's/\r$//' infra/scripts/airflow-startup.sh
```
Then re-apply Terraform.

### "bind: address already in use" on port 8080
An old container (usually from konlet, named `klt--*`) is using port 8080:
```bash
docker ps  # find the old container
docker rm -f klt--<id>
# Then start the webserver container
```

### "The conn_id `google_cloud_default` isn't defined"
The GCP connection wasn't created. Add it manually:
```bash
docker exec airflow-scheduler airflow connections add \
  google_cloud_default --conn-type google_cloud_platform
```
This is normally handled by the startup script's init step.

### Stale PID file prevents webserver from starting
```
Error: Already running on PID 19 (or pid file is stale)
```
Delete the PID file and restart:
```bash
docker exec airflow-webserver rm -f /opt/airflow/airflow-webserver.pid
docker restart airflow-webserver
```

### Permission denied on /opt/airflow/logs
The host directories need correct ownership:
```bash
sudo chown -R 50000:0 /home/airflow/dags /home/airflow/logs
```

### DAGs not appearing in the UI
1. Check the files exist: `ls /home/airflow/dags/`
2. Check ownership: `ls -ln /home/airflow/dags/` (should be UID 50000)
3. Check for import errors: `docker exec airflow-scheduler airflow dags list`

### Tasks stuck in "queued" state
With LocalExecutor this shouldn't happen often. If it does:
1. Check scheduler is running: `docker ps | grep scheduler`
2. Check scheduler logs: `docker logs airflow-scheduler --tail 30`
3. Restart scheduler: `docker restart airflow-scheduler`

## Costs

| Resource | Monthly Cost |
|----------|-------------|
| e2-medium VM (continuous) | ~$25 |
| 20 GB pd-balanced disk | ~$2 |
| **Total** | **~$27/month** |

Cloud Run job executions are billed separately (per-invocation + compute time).
