# Datadog Observability Setup

Full-stack monitoring for the STOXX platform using Datadog EU (`datadoghq.eu`).
Covers infrastructure metrics, container logs, APM traces, and GCP cloud integration.

## Architecture

```
GCE VM: stoxx-airflow (e2-medium, COS)
│
├── Docker network: airflow-net
│   ├── airflow-postgres     (postgres:16-alpine)     ← metadata DB
│   ├── airflow-webserver    (airflow:2.10.5)          ← UI on port 8080
│   ├── airflow-scheduler    (airflow:2.10.5)          ← LocalExecutor
│   ├── airflow-triggerer    (airflow:2.10.5)          ← deferred tasks
│   └── dd-agent             (gcr.io/datadoghq/agent:7) ← Datadog Agent
│       ├── port 8126 → APM traces from Cloud Run
│       ├── Docker socket → container logs + metrics
│       └── /proc, /sys → host system metrics
│
└── Sends to: Datadog EU (datadoghq.eu)
                ├── Infrastructure → VM CPU, RAM, disk
                ├── Logs → Airflow + Postgres container logs
                ├── APM → Pipeline step traces + SQL queries
                └── GCP Integration → Cloud Run, Cloud SQL metrics
```

## What Gets Monitored

| Source | Method | Data |
|--------|--------|------|
| Airflow VM | DD Agent (system checks) | CPU, RAM, disk, network, I/O |
| Airflow containers | DD Agent (Docker autodiscovery) | Per-container logs, CPU, memory |
| PostgreSQL | DD Agent (Postgres check via labels) | Connections, query metrics |
| Pipeline steps | ddtrace APM | Per-step traces with duration, SQL queries |
| Cloud Run services | GCP Integration | Request count, latency, instance count |
| Cloud Run jobs | GCP Integration | Execution count, duration, errors |
| Cloud SQL | GCP Integration | CPU, memory, connections |

## Three Pillars

Datadog organizes observability into three pillars. Each has a different collection method:

| Pillar | What | How it gets to Datadog |
|--------|------|------------------------|
| **Metrics** | Numeric time series (CPU %, memory, request count) | Agent collects from host + Docker; GCP Integration pulls from Cloud Monitoring API |
| **Logs** | Structured text from containers | Agent reads Docker stdout via socket; `LOG_FORMAT=json` enables structured parsing |
| **Traces** | Request-level spans with timing | `ddtrace-run` instruments Python code; traces route through Agent on port 8126 |

All three converge in Datadog by sharing the `service` tag (e.g., `stoxx-pipeline`) and
trace correlation IDs (`dd.trace_id`, `dd.span_id`) for log-to-trace linking.

## Prerequisites

1. **Datadog EU account** at `datadoghq.eu` (14-day trial is sufficient)
2. **API key** (32 characters) from Organization Settings > API Keys
3. **Application key** (40 characters) from Organization Settings > Application Keys
4. All existing STOXX infrastructure deployed via Terraform

> **API Key vs Application Key:** The API key (32 chars) is used to **send** data (metrics,
> logs, traces) to Datadog. The Application key (40 chars) is used to **read** data from
> Datadog's API (listing hosts, creating dashboards). The Agent and ddtrace only need the
> API key. You need the Application key for API queries (e.g., listing hosts via curl).

## Files Modified

| File | Change |
|------|--------|
| `infra/variables.tf` | Added `dd_api_key` variable (sensitive, default `""`) |
| `infra/terraform.tfvars` | Set `dd_api_key` value |
| `infra/compute.tf` | Added `dd-api-key` to VM metadata; CRLF fix for startup script |
| `infra/scripts/airflow-startup.sh` | Added dd-agent container, stale container cleanup, Autodiscovery labels |
| `infra/network.tf` | Added firewall rule for APM port 8126 |
| `infra/iam.tf` | Datadog GCP Integration service account (conditional) |
| `infra/outputs.tf` | Added `datadog_sa_key` output |
| `infra/run.tf` | APM env vars on pipeline Cloud Run Job |
| `docker/pipeline.Dockerfile` | Changed entrypoint to `ddtrace-run` |
| `requirements.txt` | Added `ddtrace>=2.10.0` |
| `utils/logger.py` | Trace correlation IDs in JSON formatter |
| `utils/run_pipeline.py` | Manual spans per pipeline step |

## Setup Steps

### 1. Create Datadog Account

Sign up at `datadoghq.eu` (select EU region). Go to Organization Settings > API Keys
and create a new API key. The key is **32 characters** — do not confuse it with the
Application key which is 40 characters.

### 2. Add Terraform Variable

The `dd_api_key` variable is already defined in `infra/variables.tf`:

```hcl
variable "dd_api_key" {
  description = "Datadog API key (leave empty to disable agent)"
  type        = string
  sensitive   = true
  default     = ""
}
```

Set it in `infra/terraform.tfvars` (git-ignored):

```hcl
dd_api_key = "<your-32-char-api-key>"
```

All Datadog resources are conditional on `var.dd_api_key != ""`. Setting it to empty
disables everything.

### 3. Apply Terraform

```bash
terraform -chdir=infra apply
```

This creates/updates:
- VM metadata with `dd-api-key` (read by startup script)
- Firewall rule `stoxx-allow-apm` (port 8126 from VPC subnet)
- Datadog service account `stoxx-datadog` with viewer roles
- Cloud Run Job env vars for APM

### 4. Restart the Airflow VM

The startup script reads the API key from VM metadata and launches dd-agent on boot:

```bash
gcloud compute instances stop stoxx-airflow --zone=europe-west1-b
gcloud compute instances start stoxx-airflow --zone=europe-west1-b
```

### 5. Verify dd-agent is Running

SSH into the VM and check:

```bash
gcloud compute ssh stoxx-airflow --zone=europe-west1-b
docker ps
```

Expected: 5 containers (4 Airflow + dd-agent):

```
NAMES               IMAGE                              STATUS
dd-agent            gcr.io/datadoghq/agent:7           Up 2 minutes
airflow-webserver   apache/airflow:2.10.5-python3.12   Up 3 minutes
airflow-scheduler   apache/airflow:2.10.5-python3.12   Up 3 minutes
airflow-triggerer   apache/airflow:2.10.5-python3.12   Up 3 minutes
airflow-postgres    postgres:16-alpine                 Up 4 minutes
```

Check agent status:

```bash
docker exec dd-agent agent status | head -30
```

### 6. Set Up GCP Integration (Datadog UI)

This enables Datadog to pull metrics from Cloud Run, Cloud SQL, and Compute Engine
via the GCP Cloud Monitoring API.

1. In Datadog, go to **Integrations > Google Cloud Platform**
2. Choose **Manual** setup method
3. Enter the service account email:
   ```
   stoxx-datadog@<PROJECT_ID>.iam.gserviceaccount.com
   ```
4. When prompted for "Generate Principal", use the SA impersonation flow
5. Enable **GCE Automuting** (auto-mutes monitors when VM is stopped)
6. Enable **Resource Collection** (discovers GCP resources in Datadog)
7. Save the integration

> The Datadog SA has `monitoring.viewer`, `compute.viewer`, and `cloudasset.viewer`
> roles — it can read metrics but cannot modify any GCP resources.

### 7. Rebuild Pipeline Image

The pipeline Dockerfile now uses `ddtrace-run` as the entrypoint. Push a new image
to trigger the change:

```bash
# Via CI (push to trigger GitHub Actions), or manually:
docker build -t europe-west1-docker.pkg.dev/<PROJECT_ID>/stoxx/pipeline:latest \
  -f docker/pipeline.Dockerfile .
docker push europe-west1-docker.pkg.dev/<PROJECT_ID>/stoxx/pipeline:latest
```

### 8. Verify in Datadog

1. **Infrastructure > Host Map** — `stoxx-airflow` should appear with CPU/memory metrics
2. **Logs > Live Tail** — Airflow container logs should stream in real-time
3. **APM > Traces** — trigger a pipeline run, then check for `stoxx-pipeline` traces

## How Each Component Works

### DD Agent on the VM

The startup script (`infra/scripts/airflow-startup.sh`) launches dd-agent as a Docker
container on the `airflow-net` network. Key configuration:

```bash
docker run -d \
  --name dd-agent \
  --network airflow-net \
  --restart unless-stopped \
  --cgroupns host --pid host \
  -p 8126:8126 \
  -e DD_API_KEY="${DD_API_KEY}" \
  -e DD_SITE="datadoghq.eu" \
  -e DD_HOSTNAME="stoxx-airflow" \
  -e DD_LOGS_ENABLED=true \
  -e DD_LOGS_CONFIG_CONTAINER_COLLECT_ALL=true \
  -e DD_APM_ENABLED=true \
  -e DD_APM_NON_LOCAL_TRAFFIC=true \
  -v /var/run/docker.sock:/var/run/docker.sock:ro \
  -v /proc/:/host/proc/:ro \
  -v /sys/fs/cgroup/:/host/sys/fs/cgroup:ro \
  -v /var/lib/datadog-agent/run:/opt/datadog-agent/run:rw \
  gcr.io/datadoghq/agent:7
```

Key details:
- **Port 8126** is exposed on the host for APM traces from Cloud Run via VPC
- **Docker socket** (read-only) enables container discovery and log collection
- **`DD_SITE=datadoghq.eu`** routes all data to the EU region
- **`/var/lib/datadog-agent/run`** is used instead of `/opt/datadog-agent/run` on
  the host because COS has a read-only `/opt` filesystem
- The agent is guarded by `|| echo "WARNING..."` so a failure doesn't block Airflow
  startup (the script uses `set -euo pipefail`)

The API key is read from VM metadata (set by Terraform):

```bash
DD_API_KEY=$(curl -sf -H "Metadata-Flavor: Google" \
  "http://metadata.google.internal/computeMetadata/v1/instance/attributes/dd-api-key" || true)
```

### Docker Autodiscovery Labels

Container log source tagging and the Postgres integration check are configured via
Docker labels on the containers, not in Datadog config files:

```bash
# On airflow-postgres:
-l com.datadoghq.ad.logs='[{"source":"postgresql","service":"airflow-postgres"}]'
-l com.datadoghq.ad.check_names='["postgres"]'
-l com.datadoghq.ad.init_configs='[{}]'
-l com.datadoghq.ad.instances='[{"host":"%%host%%","port":"5432","username":"airflow","password":"airflow"}]'

# On airflow-webserver, scheduler, triggerer:
-l com.datadoghq.ad.logs='[{"source":"airflow","service":"airflow-<component>"}]'
```

These labels tell the agent:
- Which log pipeline to use (`source` → Datadog's built-in log parsing)
- Which integration check to run (Postgres metrics collection)
- `%%host%%` resolves to the container's Docker IP at runtime

### APM Traces (ddtrace)

The pipeline runs inside Cloud Run with `ddtrace-run` wrapping the Python process:

```dockerfile
# docker/pipeline.Dockerfile
ENTRYPOINT ["ddtrace-run", "python", "utils/run_pipeline.py"]
```

`ddtrace-run` auto-instruments: pyodbc (SQL queries), urllib3 (HTTP), and logging.
Traces are sent to the DD Agent on the Airflow VM via VPC:

```hcl
# infra/run.tf — Cloud Run Job env vars
env { name = "DD_SERVICE";         value = "stoxx-pipeline" }
env { name = "DD_ENV";             value = "prod" }
env { name = "DD_TRACE_AGENT_URL"; value = "http://<VM_PRIVATE_IP>:8126" }
env { name = "DD_API_KEY";         value = var.dd_api_key }
env { name = "LOG_FORMAT";         value = "json" }
```

The trace flow: Cloud Run Job → VPC (private ranges) → VM port 8126 → dd-agent → Datadog EU.

This requires:
- VPC access on Cloud Run with `egress = "PRIVATE_RANGES_ONLY"`
- Firewall rule allowing TCP 8126 from `10.0.0.0/24` to tag `airflow`
- dd-agent with `-p 8126:8126` (host port mapping) and `DD_APM_NON_LOCAL_TRAFFIC=true`

### Manual Spans per Pipeline Step

Each pipeline step is wrapped in a trace span (`utils/run_pipeline.py`):

```python
try:
    from ddtrace import tracer
except ImportError:
    tracer = None

# In the main loop:
if tracer:
    with tracer.trace("pipeline.step", service="stoxx-pipeline",
                      resource=name) as span:
        span.set_tag("step.num", num)
        span.set_tag("step.name", name)
        fn()
else:
    fn()
```

This creates a flame graph in APM showing each step's duration, plus auto-instrumented
child spans for SQL queries (via pyodbc).

The `try/except ImportError` guard means the code works identically without ddtrace
installed (local development).

### Log-to-Trace Correlation

The JSON log formatter (`utils/logger.py`) injects trace IDs into every log line:

```python
try:
    from ddtrace import tracer
    span = tracer.current_span()
    if span:
        log["dd.trace_id"] = str(span.trace_id)
        log["dd.span_id"] = str(span.span_id)
except ImportError:
    pass
```

In Datadog, this enables clicking from a log line directly to the corresponding APM trace,
and vice versa. The correlation works because both share the same `dd.trace_id`.

### Stale Container Cleanup

The startup script removes old containers before starting new ones, preventing port
conflicts after VM reboot:

```bash
for c in airflow-webserver airflow-triggerer airflow-scheduler airflow-postgres dd-agent; do
  docker rm -f "$c" 2>/dev/null || true
done
```

Without this, `docker run -p 8080:8080` would fail with `bind: address already in use`
because the old container from the previous boot still holds the port.

## Memory Budget

The Airflow VM is an `e2-medium` (4 GB RAM). Memory allocation with dd-agent:

| Component | ~RAM |
|-----------|------|
| COS + Docker | 300 MB |
| PostgreSQL 16 | 100 MB |
| Webserver | 400 MB |
| Scheduler | 300 MB |
| Triggerer | 200 MB |
| dd-agent | 350 MB |
| **Total** | **~1,650 MB** |

Leaves ~2.35 GB headroom. Monitor via Datadog Infrastructure > Host Map.

## Debugging

### Agent not appearing in Datadog

```bash
# SSH into VM
gcloud compute ssh stoxx-airflow --zone=europe-west1-b

# Check agent is running
docker ps | grep dd-agent

# Check agent status
docker exec dd-agent agent status

# Check agent logs for errors
docker logs dd-agent --tail 50
```

Common issues:
- **No container:** API key is empty in VM metadata. Check `terraform output` and re-apply.
- **"Invalid API key":** Wrong key in terraform.tfvars. API keys are 32 chars, not 40.
- **Agent unhealthy:** Normal for the first ~2 minutes while checks initialize.

### No logs in Datadog

```bash
# Check agent log collection status
docker exec dd-agent agent status | grep -A 20 "Logs Agent"
```

Expected: `Logs: xx logs sent`. If 0:
- Docker socket may not be mounted: check `-v /var/run/docker.sock:/var/run/docker.sock:ro`
- `DD_LOGS_ENABLED` not set to `true`

In Datadog, use **Logs > Live Tail** (not Log Explorer) to see logs in real time.
New accounts may show an onboarding wizard — Live Tail bypasses it.

### APM traces not appearing

```bash
# Check APM status on agent
docker exec dd-agent agent status | grep -A 10 "APM Agent"
```

Expected: `Status: Running`, `Receiver: 0.0.0.0:8126`.

If traces from Cloud Run aren't arriving:
1. **Firewall:** verify `stoxx-allow-apm` exists allowing TCP 8126 from `10.0.0.0/24`
2. **Port mapping:** dd-agent must have `-p 8126:8126` (not just Docker network exposure)
3. **DD_APM_NON_LOCAL_TRAFFIC:** must be `true` for non-localhost connections
4. **Cloud Run VPC:** must have `egress = "PRIVATE_RANGES_ONLY"` to route to VM private IP

Test connectivity from the VM:

```bash
curl -s http://localhost:8126/info | head -5
```

Expected: JSON response with agent version info.

### COS filesystem constraints

Container-Optimized OS has a **read-only root filesystem**. Paths like `/opt` are not
writable. The dd-agent needs a writable directory for its run state:

```bash
# This fails on COS:
-v /opt/datadog-agent/run:/opt/datadog-agent/run:rw

# Use /var/lib instead:
mkdir -p /var/lib/datadog-agent/run
-v /var/lib/datadog-agent/run:/opt/datadog-agent/run:rw
```

### Windows line endings in startup script

If the VM shows `env: 'bash\r': No such file or directory`, the startup script has
Windows CRLF line endings. The fix is in `infra/compute.tf`:

```hcl
metadata = {
  startup-script = replace(file("${path.module}/scripts/airflow-startup.sh"), "\r\n", "\n")
}
```

This strips `\r` at the Terraform level before embedding the script into VM metadata.

### Ghost hosts in Infrastructure

When the API key is changed, the old agent may leave a ghost host entry. Ghost hosts
show as INACTIVE and auto-disappear after ~2 hours of no data. To list hosts via API:

```bash
curl -s -X GET "https://api.datadoghq.eu/api/v1/hosts?filter=stoxx" \
  -H "DD-API-KEY: <api-key>" \
  -H "DD-APPLICATION-KEY: <app-key>"
```

The Docker-internal PostgreSQL IP (e.g., `172.18.0.3`) also appears as a separate
host — this is normal, caused by the Autodiscovery Postgres check resolving `%%host%%`
to the container's bridge IP.

## Reversibility (Disabling Datadog)

When the trial ends or you want to remove Datadog:

1. Set `dd_api_key = ""` in `terraform.tfvars`
2. Run `terraform apply` — conditional resources are destroyed
3. SSH into VM and remove the agent: `docker rm -f dd-agent`
4. Revert Dockerfile entrypoint:
   ```dockerfile
   ENTRYPOINT ["python", "utils/run_pipeline.py"]
   ```
5. Remove `ddtrace>=2.10.0` from `requirements.txt`
6. Rebuild and push the pipeline image
7. The logger and run_pipeline trace code no-ops automatically (`ImportError` guard)

One `terraform apply` + one image rebuild cleans up everything.

## Costs

| Component | Monthly Cost |
|-----------|-------------|
| dd-agent on VM | $0 (uses existing VM resources) |
| Datadog EU (14-day trial) | $0 |
| Datadog EU (after trial, Infrastructure + Logs + APM Pro) | ~$50-80/host |

The agent itself is free — you pay for Datadog's SaaS based on host count and
log/trace volume.
