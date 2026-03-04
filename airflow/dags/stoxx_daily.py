"""stoxx_daily — Main STOXX pipeline DAG.

Runs 3x daily (09:00, 17:00, 22:00 UTC Mon-Fri) via Cloud Run.
5 task groups: ohlcv → signals_daily + signals_quarterly → gold_scores → gold_performance.
"""

from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from datetime import datetime, timedelta

default_args = {
    "project_id": "stoxx-index-intelligence",
    "region": "europe-west1",
    "job_name": "stoxx-pipeline",
    "deferrable": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
    "sla": timedelta(hours=1),
    "on_failure_callback": lambda context: None,
}


def _task(task_id, args):
    """Create a CloudRunExecuteJobOperator with pipeline step args."""
    return CloudRunExecuteJobOperator(
        task_id=task_id,
        **default_args,
        overrides={
            "container_overrides": [{
                "args": args,
            }],
        },
    )


with DAG(
    "stoxx_daily",
    description="Full STOXX pipeline: 5 task groups with parallel execution",
    schedule="0 9,17,22 * * 1-5",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["stoxx"],
) as dag:

    ohlcv = _task("ohlcv", ["--from", "1", "--to", "3"])
    signals_daily = _task("signals_daily", ["--steps", "4,5,8"])
    signals_quarterly = _task("signals_quarterly", ["--steps", "6,7,9"])
    gold_scores = _task("gold_scores", ["--from", "14", "--to", "15"])
    gold_performance = _task("gold_performance", ["--step", "16"])

    [ohlcv, signals_daily, signals_quarterly] >> gold_scores >> gold_performance
