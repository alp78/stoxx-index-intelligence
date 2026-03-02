#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# Airflow VM startup script (COS / Docker-only)
# Runs on every boot. Idempotent: skips containers that are already running.
# ============================================================================

AIRFLOW_IMAGE="apache/airflow:2.10.5-python3.12"
POSTGRES_IMAGE="postgres:16-alpine"

AIRFLOW_HOME="/home/airflow"
PG_DATA="${AIRFLOW_HOME}/pgdata"
DAGS_DIR="${AIRFLOW_HOME}/dags"
LOGS_DIR="${AIRFLOW_HOME}/logs"

PG_USER="airflow"
PG_PASS="airflow"
PG_DB="airflow"

ADMIN_USER="admin"
ADMIN_PASS="admin"

NETWORK="airflow-net"

# --------------------------------------------------------------------------
# 1. Stop stale containers from previous boot (handles reboot / reset)
# --------------------------------------------------------------------------
for c in airflow-webserver airflow-triggerer airflow-scheduler airflow-postgres dd-agent; do
  docker rm -f "$c" 2>/dev/null || true
done

# --------------------------------------------------------------------------
# 2. Create host directories with correct ownership
# --------------------------------------------------------------------------
mkdir -p "${PG_DATA}" "${DAGS_DIR}" "${LOGS_DIR}"
chown -R 999:999 "${PG_DATA}"
chown -R 50000:0 "${DAGS_DIR}" "${LOGS_DIR}"

# --------------------------------------------------------------------------
# 2. Pull images
# --------------------------------------------------------------------------
docker pull "${POSTGRES_IMAGE}"
docker pull "${AIRFLOW_IMAGE}"

# --------------------------------------------------------------------------
# 3. Create Docker network
# --------------------------------------------------------------------------
docker network inspect "${NETWORK}" >/dev/null 2>&1 \
  || docker network create "${NETWORK}"

# --------------------------------------------------------------------------
# 3b. Start Datadog Agent (skip if no API key or already running)
# --------------------------------------------------------------------------
DD_API_KEY=$(curl -sf -H "Metadata-Flavor: Google" \
  "http://metadata.google.internal/computeMetadata/v1/instance/attributes/dd-api-key" || true)

if [ -n "${DD_API_KEY}" ] && ! docker ps --format '{{.Names}}' | grep -qx 'dd-agent'; then
  docker pull gcr.io/datadoghq/agent:7
  docker rm -f dd-agent 2>/dev/null || true

  # /var/lib is writable on COS; /opt is read-only
  mkdir -p /var/lib/datadog-agent/run

  docker run -d \
    --name dd-agent \
    --network "${NETWORK}" \
    --restart unless-stopped \
    --cgroupns host \
    --pid host \
    -p 8126:8126 \
    -e DD_API_KEY="${DD_API_KEY}" \
    -e DD_SITE="datadoghq.eu" \
    -e DD_HOSTNAME="stoxx-airflow" \
    -e DD_TAGS="env:prod,service:stoxx-airflow" \
    -e DD_LOGS_ENABLED=true \
    -e DD_LOGS_CONFIG_CONTAINER_COLLECT_ALL=true \
    -e DD_CONTAINER_EXCLUDE="name:dd-agent" \
    -e DD_APM_ENABLED=true \
    -e DD_APM_NON_LOCAL_TRAFFIC=true \
    -e DD_DOGSTATSD_NON_LOCAL_TRAFFIC=true \
    -e DD_PROCESS_AGENT_ENABLED=true \
    -v /var/run/docker.sock:/var/run/docker.sock:ro \
    -v /proc/:/host/proc/:ro \
    -v /sys/fs/cgroup/:/host/sys/fs/cgroup:ro \
    -v /var/lib/datadog-agent/run:/opt/datadog-agent/run:rw \
    gcr.io/datadoghq/agent:7 || echo "WARNING: dd-agent failed to start (non-fatal)"
fi

# --------------------------------------------------------------------------
# 4. Start PostgreSQL (skip if already running)
# --------------------------------------------------------------------------
if ! docker ps --format '{{.Names}}' | grep -qx 'airflow-postgres'; then
  docker rm -f airflow-postgres 2>/dev/null || true

  docker run -d \
    --name airflow-postgres \
    --network "${NETWORK}" \
    --restart unless-stopped \
    -l com.datadoghq.ad.logs='[{"source":"postgresql","service":"airflow-postgres"}]' \
    -l com.datadoghq.ad.check_names='["postgres"]' \
    -l com.datadoghq.ad.init_configs='[{}]' \
    -l com.datadoghq.ad.instances='[{"host":"%%host%%","port":"5432","username":"airflow","password":"airflow"}]' \
    -e POSTGRES_USER="${PG_USER}" \
    -e POSTGRES_PASSWORD="${PG_PASS}" \
    -e POSTGRES_DB="${PG_DB}" \
    -v "${PG_DATA}:/var/lib/postgresql/data" \
    "${POSTGRES_IMAGE}"
fi

# --------------------------------------------------------------------------
# 5. Wait for PostgreSQL
# --------------------------------------------------------------------------
echo "Waiting for PostgreSQL..."
for i in $(seq 1 30); do
  if docker exec airflow-postgres pg_isready -U "${PG_USER}" >/dev/null 2>&1; then
    echo "PostgreSQL is ready."
    break
  fi
  sleep 2
done

# --------------------------------------------------------------------------
# 6. Common Airflow environment variables
# --------------------------------------------------------------------------
AIRFLOW_ENV=(
  -e "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://${PG_USER}:${PG_PASS}@airflow-postgres:5432/${PG_DB}"
  -e AIRFLOW__CORE__EXECUTOR=LocalExecutor
  -e AIRFLOW__CORE__LOAD_EXAMPLES=false
  -e AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
  -e AIRFLOW__CORE__DEFAULT_TIMEZONE=Europe/Paris
  -e AIRFLOW__WEBSERVER__WEB_SERVER_HOST=0.0.0.0
  -e AIRFLOW__WEBSERVER__DEFAULT_UI_TIMEZONE=Europe/Paris
)

AIRFLOW_VOLS=(
  -v "${DAGS_DIR}:/opt/airflow/dags"
  -v "${LOGS_DIR}:/opt/airflow/logs"
)

# --------------------------------------------------------------------------
# 7. DB migration + admin user (one-shot, idempotent)
# --------------------------------------------------------------------------
docker run --rm \
  --name airflow-init \
  --network "${NETWORK}" \
  "${AIRFLOW_ENV[@]}" \
  "${AIRFLOW_VOLS[@]}" \
  "${AIRFLOW_IMAGE}" \
  bash -c "
    airflow db migrate && \
    airflow users create \
      --username ${ADMIN_USER} \
      --password ${ADMIN_PASS} \
      --firstname Admin \
      --lastname User \
      --role Admin \
      --email admin@example.com \
    || true
    airflow connections add google_cloud_default --conn-type google_cloud_platform 2>/dev/null || true
  "

# --------------------------------------------------------------------------
# 8. Start webserver (skip if already running)
# --------------------------------------------------------------------------
if ! docker ps --format '{{.Names}}' | grep -qx 'airflow-webserver'; then
  docker rm -f airflow-webserver 2>/dev/null || true

  docker run -d \
    --name airflow-webserver \
    --network "${NETWORK}" \
    --restart unless-stopped \
    -l com.datadoghq.ad.logs='[{"source":"airflow","service":"airflow-webserver"}]' \
    -p 8080:8080 \
    "${AIRFLOW_ENV[@]}" \
    "${AIRFLOW_VOLS[@]}" \
    "${AIRFLOW_IMAGE}" \
    airflow webserver
fi

# --------------------------------------------------------------------------
# 9. Start scheduler (skip if already running)
# --------------------------------------------------------------------------
if ! docker ps --format '{{.Names}}' | grep -qx 'airflow-scheduler'; then
  docker rm -f airflow-scheduler 2>/dev/null || true

  docker run -d \
    --name airflow-scheduler \
    --network "${NETWORK}" \
    --restart unless-stopped \
    -l com.datadoghq.ad.logs='[{"source":"airflow","service":"airflow-scheduler"}]' \
    "${AIRFLOW_ENV[@]}" \
    "${AIRFLOW_VOLS[@]}" \
    "${AIRFLOW_IMAGE}" \
    airflow scheduler
fi

# --------------------------------------------------------------------------
# 10. Start triggerer (skip if already running)
# --------------------------------------------------------------------------
if ! docker ps --format '{{.Names}}' | grep -qx 'airflow-triggerer'; then
  docker rm -f airflow-triggerer 2>/dev/null || true

  docker run -d \
    --name airflow-triggerer \
    --network "${NETWORK}" \
    --restart unless-stopped \
    -l com.datadoghq.ad.logs='[{"source":"airflow","service":"airflow-triggerer"}]' \
    "${AIRFLOW_ENV[@]}" \
    "${AIRFLOW_VOLS[@]}" \
    "${AIRFLOW_IMAGE}" \
    airflow triggerer
fi

echo "Airflow startup complete."
