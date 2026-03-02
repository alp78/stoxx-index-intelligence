"""ESG ingestion pipeline orchestrator.

Runs all pipeline steps in the correct order. Each step is idempotent.

Usage:
  python run_pipeline.py            # Run all steps
  python run_pipeline.py --step 3   # Run only step 3
  python run_pipeline.py --from 5   # Run steps 5 through end
"""

import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import INDICES, data_path, get_all_keys, bronze_ohlcv, silver_ohlcv
from logger import get_logger, log_info, log_error, StepTimer

logger = get_logger("pipeline")


def step_01_setup_index():
    """Create missing OHLCV tables (bronze + silver)."""
    from setup_index import setup
    setup()


def step_02_load_index_dim():
    """Load dim JSON -> bronze.index_dim."""
    from loaders.load_index_dim import load
    for key in get_all_keys():
        f = data_path(key, "dim")
        if not f.exists():
            log_info(logger, "Skipping (no file)", step="pipeline", index=key, kind="dim")
            continue
        load(f, key)


def step_03_transform_calendar():
    """Seed/refresh trading calendar from exchange_calendars."""
    from transforms.transform_trading_calendar import run
    run()


def step_04_transform_index_dim():
    """SCD2 merge: bronze.index_dim -> silver.index_dim."""
    from transforms.transform_index_dim import run
    run()


def step_05_load_ohlcv_history():
    """Load OHLCV history JSON -> bronze (first run only)."""
    from loaders.load_ohlcv_history import load
    for idx in INDICES:
        key = idx["key"]
        f = data_path(key, "ohlcv_history")
        if not f.exists():
            log_info(logger, "Skipping (no file)", step="pipeline", index=key, kind="ohlcv_history")
            continue
        load(f, bronze_ohlcv(key))


def step_06_load_ohlcv_daily():
    """Load recent OHLCV JSON -> bronze."""
    from loaders.load_ohlcv_daily import load
    for idx in INDICES:
        key = idx["key"]
        f = data_path(key, "ohlcv_daily")
        if not f.exists():
            log_info(logger, "Skipping (no file)", step="pipeline", index=key, kind="ohlcv_daily")
            continue
        load(f, bronze_ohlcv(key))


def step_07_transform_ohlcv():
    """Gap-fill bronze OHLCV -> silver OHLCV."""
    from transforms.transform_ohlcv_daily import run
    run()


def step_08_load_signals_daily():
    """Load daily signals JSON -> bronze."""
    from loaders.load_signals_daily import load
    for key in get_all_keys():
        f = data_path(key, "signals_daily")
        if not f.exists():
            log_info(logger, "Skipping (no file)", step="pipeline", index=key, kind="signals_daily")
            continue
        load(f, key)


def step_09_load_signals_quarterly():
    """Load quarterly signals JSON -> bronze."""
    from loaders.load_signals_quarterly import load
    for key in get_all_keys():
        f = data_path(key, "signals_quarterly")
        if not f.exists():
            log_info(logger, "Skipping (no file)", step="pipeline", index=key, kind="signals_quarterly")
            continue
        load(f, key)


def step_10_transform_signals_daily():
    """Dedup bronze.signals_daily -> silver.signals_daily."""
    from transforms.transform_signals_daily import run
    run()


def step_11_transform_signals_quarterly():
    """Dedup bronze.signals_quarterly -> silver.signals_quarterly."""
    from transforms.transform_signals_quarterly import run
    run()


def step_12_load_pulse_tickers():
    """Load pulse tickers JSON -> bronze."""
    from loaders.load_pulse_tickers import load
    for key in get_all_keys():
        f = data_path(key, "tickers")
        if not f.exists():
            log_info(logger, "Skipping (no file)", step="pipeline", index=key, kind="tickers")
            continue
        load(f, key)


def step_13_load_pulse():
    """Load pulse JSON -> bronze."""
    from loaders.load_pulse import load
    for key in get_all_keys():
        f = data_path(key, "pulse")
        if not f.exists():
            log_info(logger, "Skipping (no file)", step="pipeline", index=key, kind="pulse")
            continue
        load(f, key)


STEPS = [
    (1,  "setup_index",              step_01_setup_index),
    (2,  "load_index_dim",           step_02_load_index_dim),
    (3,  "transform_calendar",       step_03_transform_calendar),
    (4,  "transform_index_dim",      step_04_transform_index_dim),
    (5,  "load_ohlcv_history",       step_05_load_ohlcv_history),
    (6,  "load_ohlcv_daily",         step_06_load_ohlcv_daily),
    (7,  "transform_ohlcv",          step_07_transform_ohlcv),
    (8,  "load_signals_daily",       step_08_load_signals_daily),
    (9,  "load_signals_quarterly",   step_09_load_signals_quarterly),
    (10, "transform_signals_daily",  step_10_transform_signals_daily),
    (11, "transform_signals_quarterly", step_11_transform_signals_quarterly),
    (12, "load_pulse_tickers",       step_12_load_pulse_tickers),
    (13, "load_pulse",               step_13_load_pulse),
]


def main():
    parser = argparse.ArgumentParser(description="ESG ingestion pipeline")
    parser.add_argument("--step", type=int, help="Run a single step")
    parser.add_argument("--from", type=int, dest="from_step", help="Resume from step N")
    args = parser.parse_args()

    if args.step:
        steps = [(n, name, fn) for n, name, fn in STEPS if n == args.step]
    elif args.from_step:
        steps = [(n, name, fn) for n, name, fn in STEPS if n >= args.from_step]
    else:
        steps = STEPS

    log_info(logger, "Pipeline started", step="pipeline",
             steps=len(steps), indices=len(INDICES))

    with StepTimer() as total_timer:
        failed = []
        for num, name, fn in steps:
            log_info(logger, f"Step {num}: {name}", step="pipeline", step_num=num)
            try:
                with StepTimer() as step_timer:
                    fn()
                log_info(logger, f"Step {num} complete", step="pipeline",
                         step_num=num, step_name=name,
                         duration_ms=step_timer.duration_ms)
            except Exception:
                log_error(logger, f"Step {num} failed", exc_info=True,
                          step="pipeline", step_num=num, step_name=name)
                failed.append(name)

    if failed:
        log_error(logger, "Pipeline finished with errors", step="pipeline",
                  failed_steps=failed, duration_ms=total_timer.duration_ms)
        sys.exit(1)
    else:
        log_info(logger, "Pipeline complete", step="pipeline",
                 duration_ms=total_timer.duration_ms)


if __name__ == "__main__":
    main()
