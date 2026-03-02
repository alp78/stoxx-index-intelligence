"""Removes all data for a given index from the database and filesystem.

Keeps the definition file (data/definitions/{key}.json) intact.

Usage:
  python utils/drop_index.py stoxx_asia_50
"""

import sys
import argparse
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(_PROJECT_ROOT))

from utils.db import get_connection
from utils.logger import get_logger, log_info, log_warning, log_error, StepTimer

logger = get_logger("drop_index")

# JSON data files to remove (kind -> subdir)
_DATA_FILES = {
    "dim": "dimensions",
    "signals_daily": "stage",
    "signals_quarterly": "stage",
    "ohlcv": "stage",
    "ohlcv_history": "history",
    "ohlcv_daily": "stage",
    "tickers": "pulse",
    "pulse": "pulse",
}


def drop_index(key):
    prefix = key.replace("_", "")
    ohlcv = f"{prefix}_ohlcv"

    log_info(logger, "Removing all data for index — database rows and JSON files",
             index=key, prefix=prefix, ohlcv_table=ohlcv)

    # --- 1. Database cleanup ---
    conn = get_connection()
    cursor = conn.cursor()

    try:
        with StepTimer() as timer:
            # Silver layer
            for table in ["signals_quarterly", "signals_daily", "index_dim"]:
                cursor.execute(f"DELETE FROM silver.{table} WHERE _index = ?", key)
                if cursor.rowcount > 0:
                    log_info(logger, f"Purged silver.{table} rows for this index",
                             index=key, rows_deleted=cursor.rowcount)
                else:
                    log_info(logger, f"No rows in silver.{table} for this index — already clean",
                             index=key)

            cursor.execute(f"SELECT OBJECT_ID('silver.{ohlcv}', 'U')")
            if cursor.fetchone()[0] is not None:
                cursor.execute(f"DROP TABLE silver.{ohlcv}")
                log_info(logger, f"Dropped silver OHLCV table (silver.{ohlcv})", index=key)
            else:
                log_info(logger, f"Silver OHLCV table (silver.{ohlcv}) does not exist — already dropped",
                         index=key)

            # Bronze layer
            for table in ["pulse", "pulse_tickers", "signals_quarterly",
                          "signals_daily", "index_dim"]:
                cursor.execute(f"DELETE FROM bronze.{table} WHERE _index = ?", key)
                if cursor.rowcount > 0:
                    log_info(logger, f"Purged bronze.{table} rows for this index",
                             index=key, rows_deleted=cursor.rowcount)
                else:
                    log_info(logger, f"No rows in bronze.{table} for this index — already clean",
                             index=key)

            cursor.execute(f"SELECT OBJECT_ID('bronze.{ohlcv}', 'U')")
            if cursor.fetchone()[0] is not None:
                cursor.execute(f"DROP TABLE bronze.{ohlcv}")
                log_info(logger, f"Dropped bronze OHLCV table (bronze.{ohlcv})", index=key)
            else:
                log_info(logger, f"Bronze OHLCV table (bronze.{ohlcv}) does not exist — already dropped",
                         index=key)

            # Clean up orphaned trading calendar rows
            cursor.execute("""
                DELETE FROM bronze.trading_calendar
                WHERE exchange_code NOT IN (
                    SELECT DISTINCT exchange FROM bronze.index_dim
                )
            """)
            if cursor.rowcount > 0:
                log_info(logger, "Removed trading calendar rows for exchanges no longer used by any index",
                         index=key, rows_removed=cursor.rowcount)
            else:
                log_info(logger, "No orphaned trading calendar rows — all exchanges still in use",
                         index=key)

            conn.commit()

        log_info(logger, "Database cleanup complete",
                 index=key, duration_ms=timer.duration_ms)

    except Exception as e:
        conn.rollback()
        log_error(logger, "Database cleanup failed — rolled back", index=key, error=str(e))
        raise
    finally:
        cursor.close()
        conn.close()

    # --- 2. File cleanup ---
    data_dir = _PROJECT_ROOT / "data"
    removed = 0
    already_gone = 0

    for kind, subdir in _DATA_FILES.items():
        f = data_dir / subdir / f"{prefix}_{kind}.json"
        if f.exists():
            f.unlink()
            log_info(logger, "Deleted JSON data file", index=key, file=str(f))
            removed += 1
        else:
            already_gone += 1

    if removed > 0:
        log_info(logger, "File cleanup complete — JSON data files deleted", index=key, files_deleted=removed)
    elif already_gone > 0:
        log_info(logger, "No JSON data files found — already cleaned up", index=key)
    else:
        log_info(logger, "No JSON data files to remove", index=key)

    # --- 3. Confirm definition kept ---
    def_file = data_dir / "definitions" / f"{key}.json"
    if def_file.exists():
        log_info(logger, "Definition file preserved (not deleted) — re-run setup_index to rebuild",
                 index=key, file=str(def_file))
    else:
        log_warning(logger, "No definition file found — index has no definition to re-setup from",
                    index=key, expected=str(def_file))

    log_info(logger, "Drop complete for index", index=key)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Remove all data for an index")
    parser.add_argument("index_key", help="Index key (e.g. stoxx_asia_50)")
    args = parser.parse_args()
    drop_index(args.index_key)
