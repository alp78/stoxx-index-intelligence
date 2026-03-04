"""STOXX ingestion pipeline orchestrator — daily operational steps only.

Runs all pipeline steps in the correct order: fetch -> load -> transform.
Each step checks per-index preconditions and skips indices that already
have data or whose prerequisites are missing/corrupt.

Prerequisite: run setup_index.py first to create tables, fetch dims,
and populate initial data (OHLCV history, signals, pulse).

Usage:
  python run_pipeline.py              # Run all steps
  python run_pipeline.py --step 3     # Run only step 3
  python run_pipeline.py --from 5     # Run steps 5 through end
"""

import json
import sys
import argparse
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ingestion"))
from config import INDICES, data_path, get_all_keys, bronze_ohlcv, silver_ohlcv
from db import get_connection
from logger import get_logger, log_info, log_warning, log_error, StepTimer

logger = get_logger("pipeline")

try:
    from ddtrace import tracer
except ImportError:
    tracer = None

_REBUILD_HINT = "To rebuild: python utils/drop_index.py {key} && python utils/setup_index.py {key} && python utils/run_pipeline.py"


def _preflight():
    """Log per-index status: set up vs not set up."""
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT DISTINCT _index FROM bronze.index_dim")
        loaded_indices = {r[0] for r in cursor.fetchall()}

        for idx in INDICES:
            key = idx["key"]

            if key not in loaded_indices:
                log_warning(logger, f"Index: {key} [NOT SETUP] — run setup_index.py first",
                            step="pipeline", index=key, name=idx["name"])
                continue

            table = idx["ohlcv_table"]
            cursor.execute(f"""
                SELECT COUNT(*) FROM sys.tables t
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = 'bronze' AND t.name = ?
            """, table)
            has_table = cursor.fetchone()[0] > 0

            if has_table:
                cursor.execute(f"SELECT COUNT(*) FROM bronze.{table}")
                ohlcv_count = cursor.fetchone()[0]
            else:
                ohlcv_count = 0

            status = "READY" if ohlcv_count > 0 else "NEW (no OHLCV data)"
            log_info(logger, f"Index: {key} [{status}]",
                     step="pipeline", index=key, name=idx["name"], status=status)
    finally:
        cursor.close()
        conn.close()


def _file_ok(path):
    """Check file exists and contains valid non-empty JSON."""
    if not path.exists():
        return False
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return bool(data)
    except Exception:
        return False


def _skip(key, reason):
    """Log a skipped index with rebuild instructions."""
    log_warning(logger, f"Skipping: {reason}",
                step="pipeline", index=key,
                rebuild=_REBUILD_HINT.format(key=key))


# ---------------------------------------------------------------------------
# Steps 1-3: OHLCV  (smart fetch -> load -> transform + trim)
# ---------------------------------------------------------------------------

def step_01_fetch_ohlcv():
    """Smart OHLCV fetch: detect gaps, fetch only missing data.
    Falls back to DB for stock symbols when dim file is missing (Cloud Run)."""
    from fetchers.fetch_ohlcv import fetch_ohlcv
    for idx in INDICES:
        key = idx["key"]
        try:
            fetch_ohlcv(key)
        except Exception as e:
            _skip(key, f"ohlcv fetch failed: {e}")


def step_02_load_ohlcv():
    """Load OHLCV JSON -> bronze (merge)."""
    from loaders.load_ohlcv import load
    for idx in INDICES:
        key = idx["key"]
        f = data_path(key, "ohlcv")
        if not f.exists():
            log_info(logger, "No OHLCV JSON file found — skipping bronze load for this index",
                     step="pipeline", index=key)
            continue
        if not _file_ok(f):
            _skip(key, "ohlcv file is corrupt or empty")
            continue
        try:
            load(f, bronze_ohlcv(key))
        except Exception as e:
            _skip(key, f"ohlcv load failed: {e}")


def step_03_transform_ohlcv():
    """Gap-fill bronze OHLCV -> silver OHLCV, then trim bronze."""
    from transforms.transform_ohlcv import run
    run()


# ---------------------------------------------------------------------------
# Steps 4-9: Signals  (fetch -> load -> transform)
# ---------------------------------------------------------------------------

def step_04_fetch_signals_daily():
    """Fetch daily trading signals from yfinance -> JSON."""
    from fetchers.fetch_signals_daily import fetch_daily_signals
    for idx in INDICES:
        key = idx["key"]
        try:
            fetch_daily_signals(key, data_path(key, "signals_daily"))
        except Exception as e:
            _skip(key, f"signals_daily fetch failed: {e}")


def step_05_load_signals_daily():
    """Load daily signals JSON -> bronze."""
    from loaders.load_signals_daily import load
    for key in get_all_keys():
        f = data_path(key, "signals_daily")
        if not f.exists():
            log_info(logger, "No daily signals JSON file — skipping bronze load for this index",
                     step="pipeline", index=key)
            continue
        if not _file_ok(f):
            _skip(key, "signals_daily file is corrupt or empty")
            continue
        try:
            load(f, key)
        except Exception as e:
            _skip(key, f"signals_daily load failed: {e}")


def step_06_fetch_signals_quarterly():
    """Fetch quarterly fundamental signals from yfinance -> JSON."""
    from fetchers.fetch_signals_quarterly import fetch_quarterly_fundamentals
    for idx in INDICES:
        key = idx["key"]
        try:
            fetch_quarterly_fundamentals(key, data_path(key, "signals_quarterly"))
        except Exception as e:
            _skip(key, f"signals_quarterly fetch failed: {e}")


def step_07_load_signals_quarterly():
    """Load quarterly signals JSON -> bronze."""
    from loaders.load_signals_quarterly import load
    for key in get_all_keys():
        f = data_path(key, "signals_quarterly")
        if not f.exists():
            log_info(logger, "No quarterly signals JSON file — skipping bronze load for this index",
                     step="pipeline", index=key)
            continue
        if not _file_ok(f):
            _skip(key, "signals_quarterly file is corrupt or empty")
            continue
        try:
            load(f, key)
        except Exception as e:
            _skip(key, f"signals_quarterly load failed: {e}")


def step_08_transform_signals_daily():
    """Upsert bronze.signals_daily -> silver.signals_daily."""
    from transforms.transform_signals_daily import run
    run()


def step_09_transform_signals_quarterly():
    """Upsert bronze.signals_quarterly -> silver.signals_quarterly."""
    from transforms.transform_signals_quarterly import run
    run()


# ---------------------------------------------------------------------------
# Steps 10-13: Pulse  (discover tickers -> load -> fetch pulse -> load)
# ---------------------------------------------------------------------------

def step_10_fetch_pulse_tickers():
    """Discover most active tickers from yfinance -> JSON."""
    from fetchers.fetch_pulse import discover_pulse_tickers
    for idx in INDICES:
        key = idx["key"]
        try:
            discover_pulse_tickers(key, data_path(key, "tickers"))
        except Exception as e:
            _skip(key, f"pulse ticker discovery failed: {e}")


def step_11_load_pulse_tickers():
    """Load pulse tickers JSON -> bronze."""
    from loaders.load_pulse_tickers import load
    for key in get_all_keys():
        f = data_path(key, "tickers")
        if not f.exists():
            log_info(logger, "No tickers JSON file — skipping bronze load for this index",
                     step="pipeline", index=key)
            continue
        if not _file_ok(f):
            _skip(key, "tickers file is corrupt or empty")
            continue
        try:
            load(f, key)
        except Exception as e:
            _skip(key, f"pulse tickers load failed: {e}")


def step_12_fetch_pulse():
    """Fetch real-time pulse snapshots from yfinance -> JSON.
    Falls back to DB for ticker symbols when JSON file is missing (Cloud Run)."""
    from fetchers.fetch_pulse import fetch_pulse
    for idx in INDICES:
        key = idx["key"]
        try:
            fetch_pulse(data_path(key, "tickers"), data_path(key, "pulse"),
                        idx["name"], index_key=key)
        except Exception as e:
            _skip(key, f"pulse fetch failed: {e}")


def step_13_load_pulse():
    """Load pulse JSON -> bronze."""
    from loaders.load_pulse import load
    for key in get_all_keys():
        f = data_path(key, "pulse")
        if not f.exists():
            log_info(logger, "No pulse JSON file — skipping bronze load for this index",
                     step="pipeline", index=key)
            continue
        if not _file_ok(f):
            _skip(key, "pulse file is corrupt or empty")
            continue
        try:
            load(f, key)
        except Exception as e:
            _skip(key, f"pulse load failed: {e}")


# ---------------------------------------------------------------------------
# Steps 14-16: Gold  (daily scores -> quarterly scores -> index performance)
# ---------------------------------------------------------------------------

def step_14_transform_scores_daily():
    """Compute daily analytics scores (relative value, momentum, sentiment)."""
    from transforms.transform_scores_daily import run
    run()


def step_15_transform_scores_quarterly():
    """Compute quarterly analytics scores (quality, health flags, governance)."""
    from transforms.transform_scores_quarterly import run
    run()


def step_16_transform_index_performance():
    """Compute index-level daily returns and cross-sectional aggregates."""
    from transforms.transform_index_performance import run
    run()


# ---------------------------------------------------------------------------
# Step registry and main
# ---------------------------------------------------------------------------

STEPS = [
    (1,  "fetch_ohlcv",                step_01_fetch_ohlcv),
    (2,  "load_ohlcv",                 step_02_load_ohlcv),
    (3,  "transform_ohlcv",            step_03_transform_ohlcv),
    (4,  "fetch_signals_daily",         step_04_fetch_signals_daily),
    (5,  "load_signals_daily",          step_05_load_signals_daily),
    (6,  "fetch_signals_quarterly",     step_06_fetch_signals_quarterly),
    (7,  "load_signals_quarterly",      step_07_load_signals_quarterly),
    (8,  "transform_signals_daily",     step_08_transform_signals_daily),
    (9,  "transform_signals_quarterly", step_09_transform_signals_quarterly),
    (10, "fetch_pulse_tickers",         step_10_fetch_pulse_tickers),
    (11, "load_pulse_tickers",          step_11_load_pulse_tickers),
    (12, "fetch_pulse",                 step_12_fetch_pulse),
    (13, "load_pulse",                  step_13_load_pulse),
    (14, "transform_scores_daily",      step_14_transform_scores_daily),
    (15, "transform_scores_quarterly",  step_15_transform_scores_quarterly),
    (16, "transform_index_performance", step_16_transform_index_performance),
]


def main():
    parser = argparse.ArgumentParser(description="STOXX ingestion pipeline")
    parser.add_argument("--step", type=int, help="Run a single step")
    parser.add_argument("--steps", type=str, help="Comma-separated step numbers (e.g., 4,5,8)")
    parser.add_argument("--from", type=int, dest="from_step", help="Resume from step N")
    parser.add_argument("--to", type=int, dest="to_step", help="Stop after step N (use with --from)")
    args = parser.parse_args()

    if args.steps:
        step_nums = [int(s) for s in args.steps.split(",")]
        steps = [(n, name, fn) for n, name, fn in STEPS if n in step_nums]
    elif args.step:
        steps = [(n, name, fn) for n, name, fn in STEPS if n == args.step]
    elif args.from_step:
        lo = args.from_step
        hi = args.to_step or STEPS[-1][0]
        steps = [(n, name, fn) for n, name, fn in STEPS if lo <= n <= hi]
    else:
        steps = STEPS

    log_info(logger, "Daily pipeline started — running operational fetch/load/transform steps",
             step="pipeline", steps=len(steps), indices=len(INDICES))
    _preflight()

    with StepTimer() as total_timer:
        failed = []
        for num, name, fn in steps:
            log_info(logger, f"Running step {num}/{len(steps)}: {name}", step="pipeline", step_num=num)
            try:
                with StepTimer() as step_timer:
                    if tracer:
                        with tracer.trace("pipeline.step", service="stoxx-pipeline",
                                          resource=name) as span:
                            span.set_tag("step.num", num)
                            span.set_tag("step.name", name)
                            fn()
                    else:
                        fn()
                log_info(logger, f"Step {num} ({name}) complete", step="pipeline",
                         step_num=num, step_name=name,
                         duration_ms=step_timer.duration_ms)
            except Exception:
                log_error(logger, f"Step {num} ({name}) failed", exc_info=True,
                          step="pipeline", step_num=num, step_name=name)
                failed.append(name)

    if failed:
        log_error(logger, "Daily pipeline finished with errors — check failed steps above",
                  step="pipeline", failed_steps=failed,
                  duration_ms=total_timer.duration_ms)
        sys.exit(1)
    else:
        log_info(logger, "Daily pipeline complete — all steps succeeded",
                 step="pipeline", duration_ms=total_timer.duration_ms)


if __name__ == "__main__":
    main()
