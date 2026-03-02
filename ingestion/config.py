"""Centralized index configuration. All scripts read from indices.json via this module."""

import json
from pathlib import Path

_CONFIG_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _CONFIG_DIR.parent

# Load indices once at import time
with open(_CONFIG_DIR / "indices.json", "r", encoding="utf-8") as f:
    INDICES = json.load(f)

_BY_KEY = {idx["key"]: idx for idx in INDICES}

# Data directory mapping: kind -> subdirectory under data/
_DATA_DIRS = {
    "dim": "stage",
    "signals_daily": "stage",
    "signals_quarterly": "stage",
    "ohlcv_history": "history",
    "ohlcv_daily": "history",
    "pulse": "pulse",
    "tickers": "pulse",
}


def get_index(key):
    """Get full config dict for an index by key."""
    return _BY_KEY[key]


def get_all_keys():
    """Return list of all index keys."""
    return [idx["key"] for idx in INDICES]


def bronze_ohlcv(key):
    """Return bronze OHLCV table name, e.g. 'bronze.eurostoxx50_ohlcv'."""
    return f"bronze.{_BY_KEY[key]['ohlcv_table']}"


def silver_ohlcv(key):
    """Return silver OHLCV table name, e.g. 'silver.eurostoxx50_ohlcv'."""
    return f"silver.{_BY_KEY[key]['ohlcv_table']}"


def data_path(key, kind):
    """Resolve data file path for an index and data kind.

    Kinds: dim, ohlcv_history, ohlcv_daily, signals_daily,
           signals_quarterly, pulse, tickers
    """
    idx = _BY_KEY[key]
    prefix = idx["file_prefix"]
    subdir = _DATA_DIRS[kind]
    filename = f"{prefix}_{kind}.json"
    return _PROJECT_ROOT / "data" / subdir / filename
