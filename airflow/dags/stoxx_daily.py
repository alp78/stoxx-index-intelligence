from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from datetime import datetime

default_args = {
    "project_id": "stoxx-index-intelligence",
    "region": "europe-west1",
    "job_name": "stoxx-pipeline",
    "deferrable": False,
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
    description="Full STOXX pipeline: 7 task groups with parallel execution",
    schedule="0 9,17,22 * * 1-5",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["stoxx"],
) as dag:

    ohlcv = _task("ohlcv", ["--from", "1", "--to", "3"])
    signals_daily = _task("signals_daily", ["--steps", "4,5,8"])
    signals_quarterly = _task("signals_quarterly", ["--steps", "6,7,9"])
    pulse_tickers = _task("pulse_tickers", ["--from", "10", "--to", "11"])
    pulse = _task("pulse", ["--from", "12", "--to", "13"])
    gold_scores = _task("gold_scores", ["--from", "14", "--to", "15"])
    gold_performance = _task("gold_performance", ["--step", "16"])

    # ohlcv, signals_daily, signals_quarterly, pulse_tickers start in parallel
    [ohlcv, signals_daily, signals_quarterly] >> gold_scores >> gold_performance
    pulse_tickers >> pulse
