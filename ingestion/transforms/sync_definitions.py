"""Sync definition files with dimension JSONs and database.

Runs as the first pipeline step to ensure dimensions stay in sync:
  1. NEW symbols (in definition but not in dim JSON) → fetch from yfinance,
     append to dim JSON, reload bronze dim + SCD2 transform.
  2. REMOVED symbols (in dim JSON / DB but not in definition) → delete from
     dim JSON and purge from all bronze, silver, and gold tables.
"""

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from utils.db import get_connection
from utils.config import INDICES, definition_path, data_path, bronze_ohlcv, silver_ohlcv
from utils.logger import get_logger, log_info, log_warning, log_error, StepTimer

logger = get_logger(__name__)

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


def _get_dim_symbols(key):
    """Read symbols from the dimension JSON file. Returns (set, list) or (None, None)."""
    path = data_path(key, "dim")
    if not path.exists():
        return None, None
    with open(path, "r", encoding="utf-8") as f:
        records = json.load(f)
    symbols = {r["symbol"] for r in records if "symbol" in r}
    return symbols, records


def _get_db_symbols(cursor, key):
    """Get all symbols currently in bronze.index_dim for an index."""
    cursor.execute(
        "SELECT DISTINCT symbol FROM bronze.index_dim WHERE _index = ?", key
    )
    return {row[0] for row in cursor.fetchall()}


def _table_exists(cursor, full_name):
    """Check if a schema.table exists in the database."""
    cursor.execute("SELECT OBJECT_ID(?, 'U')", full_name)
    return cursor.fetchone()[0] is not None


def _fetch_new_symbols(key, new_symbols):
    """Fetch dimension data from yfinance for new symbols and append to dim JSON."""
    from fetchers.fetch_index_dim import extract_full_identity
    from utils.config import get_index, safe_write_json

    idx = get_index(key)
    dim_path = data_path(key, "dim")

    # Load existing records
    if dim_path.exists():
        with open(dim_path, "r", encoding="utf-8") as f:
            records = json.load(f)
    else:
        records = []

    added = []
    for symbol in sorted(new_symbols):
        log_info(logger, f"Fetching dimensions for new symbol {symbol}",
                 step="sync", index=key, symbol=symbol)
        record = extract_full_identity(symbol, idx["history_start"])
        if record:
            records.append(record)
            added.append(symbol)
            log_info(logger, f"Added {symbol} to dimension registry",
                     step="sync", index=key, symbol=symbol)
        else:
            log_warning(logger, f"Could not fetch dimensions for {symbol} — yfinance enrichment failed or stock too recent",
                        step="sync", index=key, symbol=symbol)
        time.sleep(0.2)

    if added:
        dim_path.parent.mkdir(parents=True, exist_ok=True)
        safe_write_json(dim_path, records)
        log_info(logger, f"Dimension JSON updated with {len(added)} new symbol(s)",
                 step="sync", index=key, added=sorted(added))

    return added


def _remove_from_dim_json(key, removed_symbols):
    """Remove symbols from the dimension JSON file."""
    from utils.config import safe_write_json

    dim_path = data_path(key, "dim")
    if not dim_path.exists():
        return

    with open(dim_path, "r", encoding="utf-8") as f:
        records = json.load(f)

    before = len(records)
    records = [r for r in records if r.get("symbol") not in removed_symbols]
    after = len(records)

    if before != after:
        safe_write_json(dim_path, records)
        log_info(logger, f"Removed {before - after} symbol(s) from dimension JSON",
                 step="sync", index=key, removed=sorted(removed_symbols))


def _purge_from_db(cursor, conn, key, removed_symbols):
    """Delete removed symbols from all bronze, silver, and gold tables."""
    for symbol in sorted(removed_symbols):
        for table in _SYMBOL_TABLES:
            cursor.execute(
                f"DELETE FROM {table} WHERE _index = ? AND symbol = ?",
                key, symbol
            )

        for ohlcv_table in [bronze_ohlcv(key), silver_ohlcv(key)]:
            if _table_exists(cursor, ohlcv_table):
                cursor.execute(
                    f"DELETE FROM {ohlcv_table} WHERE symbol = ?",
                    symbol
                )

        log_info(logger, f"Purged {symbol} from database",
                 step="sync", index=key, symbol=symbol)

    conn.commit()


def _reload_dims(keys_to_reload):
    """Reload bronze dims and run SCD2 transform for indices that changed."""
    from loaders.load_index_dim import load as load_dim
    from transforms.transform_index_dim import run as transform_dim

    for key in keys_to_reload:
        dim_file = data_path(key, "dim")
        if dim_file.exists():
            try:
                load_dim(dim_file, key)
            except Exception as e:
                log_error(logger, f"Failed to reload dimensions into bronze: {e}",
                          step="sync", index=key)

    try:
        transform_dim()
    except Exception as e:
        log_error(logger, f"SCD2 dimension transform failed: {e}", step="sync")


def run():
    """Compare definitions vs dim JSONs vs DB and sync everything."""
    conn = get_connection()
    cursor = conn.cursor()
    keys_to_reload = []

    try:
        total_added = 0
        total_purged = 0

        for idx in INDICES:
            key = idx["key"]
            def_symbols = _get_definition_symbols(key)
            if def_symbols is None:
                log_warning(logger, "Definition file not found — skipping sync",
                            step="sync", index=key)
                continue

            dim_symbols, _ = _get_dim_symbols(key)
            db_symbols = _get_db_symbols(cursor, key)

            # Use the union of dim + DB as "known" symbols
            known_symbols = (dim_symbols or set()) | db_symbols

            new_symbols = def_symbols - known_symbols
            removed_symbols = known_symbols - def_symbols

            if not new_symbols and not removed_symbols:
                log_info(logger, "Definitions in sync — no changes detected",
                         step="sync", index=key,
                         def_count=len(def_symbols), dim_count=len(dim_symbols or set()),
                         db_count=len(db_symbols))
                continue

            with StepTimer() as timer:
                # Handle new symbols
                if new_symbols:
                    log_info(logger, f"Detected {len(new_symbols)} new symbol(s)",
                             step="sync", index=key, symbols=sorted(new_symbols))
                    added = _fetch_new_symbols(key, new_symbols)
                    total_added += len(added)
                    if added:
                        keys_to_reload.append(key)

                # Handle removed symbols
                if removed_symbols:
                    log_warning(logger, f"Detected {len(removed_symbols)} removed symbol(s)",
                                step="sync", index=key, symbols=sorted(removed_symbols))
                    _remove_from_dim_json(key, removed_symbols)
                    _purge_from_db(cursor, conn, key, removed_symbols)
                    total_purged += len(removed_symbols)
                    if key not in keys_to_reload:
                        keys_to_reload.append(key)

            log_info(logger, f"Sync complete for {key}",
                     step="sync", index=key,
                     added=len(new_symbols), purged=len(removed_symbols),
                     duration_ms=timer.duration_ms)

        # Reload bronze dims + SCD2 for any changed indices
        if keys_to_reload:
            log_info(logger, f"Reloading dimensions for {len(keys_to_reload)} index(es)",
                     step="sync", indices=keys_to_reload)
            _reload_dims(keys_to_reload)

        if total_added == 0 and total_purged == 0:
            log_info(logger, "All indices in sync — no changes", step="sync")
        else:
            log_info(logger, f"Sync complete — {total_added} added, {total_purged} purged",
                     step="sync", total_added=total_added, total_purged=total_purged)

    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    run()
