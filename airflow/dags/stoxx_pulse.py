"""stoxx_pulse — Real-time pulse snapshot DAG.

Runs every 5 minutes (Mon-Fri 00:00-21:00 UTC) to fetch live price/book/volume
snapshots for pre-discovered active tickers. Feeds the Live dashboard page.
"""

from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from datetime import datetime, timedelta

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=4),
}

with DAG(
    "stoxx_pulse",
    description="Real-time pulse snapshots for live dashboard",
    schedule="*/5 0-21 * * 1-5",  # Every 5 min, 00:00-21:00 UTC Mon-Fri (covers Asia/Europe/US)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["stoxx"],
    default_args=default_args,
) as dag:

    fetch_and_load_pulse = CloudRunExecuteJobOperator(
        task_id="fetch_and_load_pulse",
        project_id="stoxx-index-intelligence",
        region="europe-west1",
        job_name="stoxx-pipeline",
        overrides={
            "container_overrides": [{
                "args": ["--from", "12", "--to", "13"],
            }],
        },
        deferrable=False,
    )
