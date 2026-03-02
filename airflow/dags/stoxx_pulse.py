from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from datetime import datetime

with DAG(
    "stoxx_pulse",
    description="Real-time pulse snapshots for live dashboard",
    schedule="*/5 0-21 * * 1-5",  # Every 5 min, 00:00-21:00 UTC Mon-Fri (covers Asia/Europe/US)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["stoxx"],
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
