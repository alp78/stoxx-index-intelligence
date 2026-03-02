from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from datetime import datetime

with DAG(
    "stoxx_tickers",
    description="Hourly refresh of most active tickers per index",
    schedule="0 0-21 * * 1-5",  # Every hour, 00:00-21:00 UTC Mon-Fri
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["stoxx"],
) as dag:

    fetch_and_load_tickers = CloudRunExecuteJobOperator(
        task_id="fetch_and_load_tickers",
        project_id="stoxx-index-intelligence",
        region="europe-west1",
        job_name="stoxx-pipeline",
        overrides={
            "container_overrides": [{
                "args": ["--from", "10", "--to", "11"],
            }],
        },
        deferrable=False,
    )
