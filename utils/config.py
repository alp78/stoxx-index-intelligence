"""Centralized index configuration. Auto-discovers indices from definitions/*.json."""

import json
import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

_CONFIG_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _CONFIG_DIR.parent
_DEFINITIONS_DIR = _PROJECT_ROOT / "data" / "definitions"

_DEFAULT_HISTORY_START = "2021-01-01"

# Data directory mapping: kind -> subdirectory under data/
_DATA_DIRS = {
    "dim": "dimensions",
    "signals_daily": "stage",
    "signals_quarterly": "stage",
    "ohlcv": "stage",
    "ohlcv_history": "history",
    "ohlcv_daily": "stage",
    "pulse": "pulse",
    "tickers": "pulse",
}


def _load_definitions():
    """Scan definitions/*.json and build the INDICES list."""
    indices = []
    if not _DEFINITIONS_DIR.is_dir():
        return indices

    for def_file in sorted(_DEFINITIONS_DIR.glob("*.json")):
        with open(def_file, "r", encoding="utf-8") as f:
            defn = json.load(f)

        if "name" not in defn:
            raise ValueError(f"Definition {def_file.name} missing required 'name' field")

        key = def_file.stem
        file_prefix = defn.get("file_prefix", key.replace("_", ""))
        ohlcv_table = f"{file_prefix}_ohlcv"
        history_start = defn.get("history_start", _DEFAULT_HISTORY_START)

        indices.append({
            "key": key,
            "name": defn["name"],
            "file_prefix": file_prefix,
            "ohlcv_table": ohlcv_table,
            "history_start": history_start,
        })

    return indices


INDICES = _load_definitions()
_BY_KEY = {idx["key"]: idx for idx in INDICES}


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


def definition_path(key):
    """Return the path to the definition file for an index."""
    return _DEFINITIONS_DIR / f"{key}.json"


def format_epoch(epoch_val, is_ms=False):
    """Safely converts Unix timestamps to YYYY-MM-DD, bypassing Windows OS limits."""
    if epoch_val is None:
        return None
    try:
        sec = epoch_val / 1000.0 if is_ms else float(epoch_val)
        epoch_dt = datetime(1970, 1, 1)
        return (epoch_dt + timedelta(seconds=sec)).strftime('%Y-%m-%d')
    except Exception:
        return None


def cet_now_str(fmt='%Y-%m-%d %H:%M:%S'):
    """Return current CET/CEST time as a formatted string."""
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("Europe/Paris")).strftime(fmt)


def safe_write_json(filepath, data):
    """Write JSON atomically: write to temp file, then rename.

    Prevents partial/corrupt writes from destroying the previous good file.
    """
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=filepath.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        os.replace(tmp, filepath)
    except Exception:
        os.unlink(tmp)
        raise
"" 
"" 
