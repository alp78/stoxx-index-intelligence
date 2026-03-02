from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from datetime import datetime

default_args = {
    "project_id": "stoxx-index-intelligence",
    "region": "europe-west1",
    "job_name": "stoxx-pipeline",
    "deferrable": False,
}

with DAG(
    "stoxx_daily",
    description="Full STOXX pipeline: 3 staggered runs after each region closes",
    schedule="0 9,17,22 * * 1-5",  # 09:00 (post-Asia), 17:00 (post-Europe), 22:00 (post-US) UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["stoxx"],
) as dag:

    run_pipeline = CloudRunExecuteJobOperator(
        task_id="run_pipeline",
        **default_args,
    )
