"""stoxx_tickers — Hourly active ticker discovery DAG.

Ranks all index stocks by volume surge + range intensity, saves top 10 per index.
These tickers are then used by stoxx_pulse for high-frequency snapshot fetching.
"""

from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from datetime import datetime, timedelta

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=15),
}

with DAG(
    "stoxx_tickers",
    description="Hourly refresh of most active tickers per index",
    schedule="0 0-21 * * 1-5",  # Every hour, 00:00-21:00 UTC Mon-Fri
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["stoxx"],
    default_args=default_args,
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
