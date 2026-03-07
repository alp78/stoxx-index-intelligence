"""Purge symbols removed from definition files.

Compares the symbols in each index's definition JSON against what exists
in the database. Any symbol present in the DB but missing from the definition
is deleted from all bronze, silver, and gold tables, plus its rows in the
shared OHLCV tables.

Runs as the first pipeline step so downstream transforms never see stale data.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from utils.db import get_connection
from utils.config import INDICES, definition_path, bronze_ohlcv, silver_ohlcv
from utils.logger import get_logger, log_info, log_warning, StepTimer

logger = get_logger(__name__)

# Tables with (_index, symbol) columns to purge from
_SYMBOL_TABLES = [
    "gold.scores_daily",
    "gold.scores_quarterly",
    "silver.signals_daily",
    "silver.signals_quarterly",
    "silver.index_dim",
    "bronze.signals_daily",
    "bronze.signals_quarterly",
    "bronze.index_dim",
]


def _get_definition_symbols(key):
    """Read the current symbol list from the definition JSON."""
    path = definition_path(key)
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        defn = json.load(f)
    return set(defn.get("symbols", []))


def _get_db_symbols(cursor, key):
    """Get all symbols currently in silver.index_dim for an index."""
    cursor.execute(
        "SELECT DISTINCT symbol FROM silver.index_dim WHERE _index = ?", key
    )
    return {row[0] for row in cursor.fetchall()}


def _table_exists(cursor, full_name):
    """Check if a schema.table exists in the database."""
    cursor.execute(
        "SELECT OBJECT_ID(?, 'U')", full_name
    )
    return cursor.fetchone()[0] is not None


def run():
    """Compare definitions vs DB and purge removed symbols."""
    conn = get_connection()
    cursor = conn.cursor()

    try:
        total_purged = 0

        for idx in INDICES:
            key = idx["key"]
            def_symbols = _get_definition_symbols(key)
            if def_symbols is None:
                log_warning(logger, "Definition file not found — skipping purge",
                            step="purge", index=key)
                continue

            db_symbols = _get_db_symbols(cursor, key)
            removed = db_symbols - def_symbols

            if not removed:
                log_info(logger, "No removed symbols detected",
                         step="purge", index=key, db_count=len(db_symbols),
                         def_count=len(def_symbols))
                continue

            log_warning(logger, f"Purging {len(removed)} removed symbol(s)",
                        step="purge", index=key, symbols=sorted(removed))

            with StepTimer() as timer:
                for symbol in sorted(removed):
                    # Delete from all standard tables
                    for table in _SYMBOL_TABLES:
                        cursor.execute(
                            f"DELETE FROM {table} WHERE _index = ? AND symbol = ?",
                            key, symbol
                        )

                    # Delete rows from shared OHLCV tables (bronze + silver)
                    for ohlcv_table in [bronze_ohlcv(key), silver_ohlcv(key)]:
                        if _table_exists(cursor, ohlcv_table):
                            cursor.execute(
                                f"DELETE FROM {ohlcv_table} WHERE symbol = ?",
                                symbol
                            )

                    log_info(logger, f"Purged {symbol}",
                             step="purge", index=key, symbol=symbol)

                conn.commit()
                total_purged += len(removed)

            log_info(logger, f"Purge complete for {key}",
                     step="purge", index=key, purged=len(removed),
                     duration_ms=timer.duration_ms)

        if total_purged == 0:
            log_info(logger, "No symbols to purge across all indices", step="purge")
        else:
            log_info(logger, f"Total symbols purged: {total_purged}", step="purge",
                     total_purged=total_purged)

    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    run()
