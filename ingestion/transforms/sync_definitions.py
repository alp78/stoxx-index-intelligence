"""Sync definition files with dimension JSONs, database, OHLCV tables, and logos.

Runs as the first pipeline step to keep everything in sync with definitions:

  NEW INDEX (definition file added):
    - Create bronze + silver OHLCV tables
    - Fetch dims from yfinance, write dim JSON, load bronze, SCD2
    - Download logos from logo.dev

  NEW SYMBOL (added to existing definition):
    - Fetch dim from yfinance, append to dim JSON, reload bronze, SCD2
    - Download logo from logo.dev

  REMOVED SYMBOL (removed from definition):
    - Remove from dim JSON
    - Purge from all bronze, silver, gold tables + OHLCV rows
    - Delete logo file

  REMOVED INDEX (definition file deleted):
    - Drop bronze + silver OHLCV tables
    - Delete from all shared tables (index_dim, signals, scores, etc.)
    - Delete dim JSON and all data files
    - Delete entire logo folder
"""

import json
import os
import shutil
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from utils.db import get_connection
from utils.config import (
    INDICES, definition_path, data_path, bronze_ohlcv, silver_ohlcv, safe_write_json,
    get_index
)
from utils.logger import get_logger, log_info, log_warning, log_error, StepTimer

logger = get_logger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_LOGOS_DIR = Path(os.getenv("LOGOS_DIR",
                  str(_PROJECT_ROOT / "dashboard" / "ESG.Dashboard" / "wwwroot" / "logos")))

# Tables with (_index, symbol) columns
_SYMBOL_TABLES = [
    "gold.scores_daily",
    "gold.scores_quarterly",
    "gold.index_performance",
    "silver.signals_daily",
    "silver.signals_quarterly",
    "silver.index_dim",
    "bronze.signals_daily",
    "bronze.signals_quarterly",
    "bronze.index_dim",
]

# OHLCV DDL templates (same as setup_index.py)
_BRONZE_OHLCV_DDL = """
IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'bronze' AND t.name = '{table}')
BEGIN
    CREATE TABLE bronze.{table} (
        id                      INT IDENTITY(1,1) PRIMARY KEY,
        _ingested_at            DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
        symbol                  VARCHAR(20)     NOT NULL,
        date                    DATE            NOT NULL,
        [open]                  FLOAT,
        high                    FLOAT,
        low                     FLOAT,
        [close]                 FLOAT,
        adj_close               FLOAT,
        volume                  BIGINT,
        dividends               FLOAT,
        stock_splits            FLOAT
    );
    CREATE INDEX IX_bronze_{table}_symbol_date ON bronze.{table} (symbol, date);
END
"""

_SILVER_OHLCV_DDL = """
IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'silver' AND t.name = '{table}')
BEGIN
    CREATE TABLE silver.{table} (
        id                      INT IDENTITY(1,1) PRIMARY KEY,
        symbol                  VARCHAR(20)     NOT NULL,
        date                    DATE            NOT NULL,
        [open]                  FLOAT,
        high                    FLOAT,
        low                     FLOAT,
        [close]                 FLOAT,
        adj_close               FLOAT,
        volume                  BIGINT,
        dividends               FLOAT,
        stock_splits            FLOAT,
        is_filled               BIT             NOT NULL DEFAULT 0
    );
    CREATE UNIQUE INDEX IX_silver_{table}_symbol_date ON silver.{table} (symbol, date);
END
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_definition_symbols(key):
    path = definition_path(key)
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        defn = json.load(f)
    return set(defn.get("symbols", []))


def _get_dim_symbols(key):
    path = data_path(key, "dim")
    if not path.exists():
        return None, None
    with open(path, "r", encoding="utf-8") as f:
        records = json.load(f)
    symbols = {r["symbol"] for r in records if "symbol" in r}
    return symbols, records


def _get_db_indices(cursor):
    """Get all distinct index keys from bronze.index_dim."""
    cursor.execute("SELECT DISTINCT _index FROM bronze.index_dim")
    return {row[0] for row in cursor.fetchall()}


def _get_db_symbols(cursor, key):
    cursor.execute(
        "SELECT DISTINCT symbol FROM bronze.index_dim WHERE _index = ?", key
    )
    return {row[0] for row in cursor.fetchall()}


def _table_exists(cursor, full_name):
    cursor.execute("SELECT OBJECT_ID(?, 'U')", full_name)
    return cursor.fetchone()[0] is not None


# ---------------------------------------------------------------------------
# OHLCV table bootstrap
# ---------------------------------------------------------------------------

def _ensure_ohlcv_tables(cursor, conn, idx):
    """Create bronze + silver OHLCV tables if they don't exist."""
    table = idx["ohlcv_table"]
    created = False

    for schema, ddl in [("bronze", _BRONZE_OHLCV_DDL), ("silver", _SILVER_OHLCV_DDL)]:
        if not _table_exists(cursor, f"{schema}.{table}"):
            cursor.execute(ddl.format(table=table))
            conn.commit()
            created = True
            log_info(logger, f"Created {schema}.{table}",
                     step="sync", index=idx["key"], table=f"{schema}.{table}")

    return created


# ---------------------------------------------------------------------------
# Index metadata upsert
# ---------------------------------------------------------------------------

def _upsert_dim_index(cursor, conn):
    """Upsert all index metadata from definition files into bronze.dim_index."""
    if not _table_exists(cursor, "bronze.dim_index"):
        return

    for idx in INDICES:
        cursor.execute("""
            MERGE bronze.dim_index AS t
            USING (SELECT ? AS index_key, ? AS display_name, ? AS file_prefix,
                          ? AS color, ? AS currency) AS s
            ON t.index_key = s.index_key
            WHEN MATCHED THEN UPDATE SET
                display_name = s.display_name, file_prefix = s.file_prefix,
                color = s.color, currency = s.currency
            WHEN NOT MATCHED THEN INSERT (index_key, display_name, file_prefix, color, currency)
                VALUES (s.index_key, s.display_name, s.file_prefix, s.color, s.currency);
        """, idx["key"], idx["name"], idx["file_prefix"],
             idx.get("color"), idx.get("currency", ""))

    # Remove entries for deleted indices
    definition_keys = {i["key"] for i in INDICES}
    cursor.execute("SELECT index_key FROM bronze.dim_index")
    for row in cursor.fetchall():
        if row[0] not in definition_keys:
            cursor.execute("DELETE FROM bronze.dim_index WHERE index_key = ?", row[0])

    conn.commit()
    log_info(logger, f"Synced {len(INDICES)} index(es) to bronze.dim_index", step="sync")


# ---------------------------------------------------------------------------
# Dimension fetch / update
# ---------------------------------------------------------------------------

def _export_db_dims(key):
    """Export existing dimension records from bronze.index_dim to rebuild the dim JSON."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM bronze.index_dim WHERE _index = ? ORDER BY symbol", key
    )
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    # Skip internal columns (_index, _ingested_at, id)
    skip = {"id", "_index", "_ingested_at"}
    records = []
    for row in rows:
        record = {}
        for col, val in zip(columns, row):
            if col in skip or val is None:
                continue
            # Convert date/datetime to ISO string for JSON serialization
            if hasattr(val, "isoformat"):
                val = val.isoformat()
            record[col] = val
        records.append(record)

    return records


def _fetch_new_symbols(key, new_symbols):
    """Fetch dimension data from yfinance for new symbols and append to dim JSON."""
    from fetchers.fetch_index_dim import extract_full_identity
    from utils.config import get_index

    idx = get_index(key)
    dim_path = data_path(key, "dim")

    if dim_path.exists():
        with open(dim_path, "r", encoding="utf-8") as f:
            records = json.load(f)
    else:
        # Dim JSON missing (e.g. ephemeral GCP container) — rebuild from DB
        records = _export_db_dims(key)
        if records:
            log_info(logger, f"Rebuilt dim JSON from DB ({len(records)} existing records)",
                     step="sync", index=key)

    added = []
    for symbol in sorted(new_symbols):
        log_info(logger, f"Fetching dimensions for new symbol {symbol}",
                 step="sync", index=key, symbol=symbol)
        record = extract_full_identity(symbol, idx["history_start"])
        if record:
            records.append(record)
            added.append(symbol)
        else:
            log_warning(logger, f"Could not fetch dimensions for {symbol}",
                        step="sync", index=key, symbol=symbol)
        time.sleep(0.2)

    if added:
        dim_path.parent.mkdir(parents=True, exist_ok=True)
        safe_write_json(dim_path, records)
        log_info(logger, f"Dimension JSON updated with {len(added)} new symbol(s)",
                 step="sync", index=key, added=sorted(added))

    return added


def _remove_from_dim_json(key, removed_symbols):
    dim_path = data_path(key, "dim")
    if not dim_path.exists():
        return

    with open(dim_path, "r", encoding="utf-8") as f:
        records = json.load(f)

    before = len(records)
    records = [r for r in records if r.get("symbol") not in removed_symbols]

    if len(records) != before:
        safe_write_json(dim_path, records)
        log_info(logger, f"Removed {before - len(records)} symbol(s) from dimension JSON",
                 step="sync", index=key, removed=sorted(removed_symbols))


# ---------------------------------------------------------------------------
# DB purge (symbols)
# ---------------------------------------------------------------------------

def _purge_symbols_from_db(cursor, conn, key, removed_symbols):
    for symbol in sorted(removed_symbols):
        for table in _SYMBOL_TABLES:
            if _table_exists(cursor, table):
                cursor.execute(
                    f"DELETE FROM {table} WHERE _index = ? AND symbol = ?",
                    key, symbol
                )

        for ohlcv_table in [bronze_ohlcv(key), silver_ohlcv(key)]:
            if _table_exists(cursor, ohlcv_table):
                cursor.execute(
                    f"DELETE FROM {ohlcv_table} WHERE symbol = ?", symbol
                )

        log_info(logger, f"Purged {symbol} from database",
                 step="sync", index=key, symbol=symbol)

    conn.commit()


# ---------------------------------------------------------------------------
# Index removal (full teardown)
# ---------------------------------------------------------------------------

def _purge_index(cursor, conn, key):
    """Completely remove an index: drop OHLCV tables, delete from shared tables, clean files."""
    log_warning(logger, f"Removing entire index {key} — definition file no longer exists",
                step="sync", index=key)

    # Derive table name the same way config.py does
    file_prefix = key.replace("_", "")
    ohlcv_table = f"{file_prefix}_ohlcv"

    for schema in ["bronze", "silver"]:
        full_name = f"{schema}.{ohlcv_table}"
        if _table_exists(cursor, full_name):
            cursor.execute(f"DROP TABLE {full_name}")
            log_info(logger, f"Dropped {full_name}", step="sync", index=key)

    # Delete from all shared tables
    for table in _SYMBOL_TABLES:
        if _table_exists(cursor, table):
            cursor.execute(f"DELETE FROM {table} WHERE _index = ?", key)

    conn.commit()
    log_info(logger, f"Purged all DB data for index {key}", step="sync", index=key)

    # Delete dim JSON
    dim_path = _PROJECT_ROOT / "data" / "dimensions" / f"{file_prefix}_dim.json"
    if dim_path.exists():
        dim_path.unlink()
        log_info(logger, f"Deleted dimension file {dim_path.name}", step="sync", index=key)

    # Delete all stage/pulse data files
    for kind in ["ohlcv", "signals_daily", "signals_quarterly", "pulse", "tickers"]:
        subdir = {"ohlcv": "stage", "signals_daily": "stage", "signals_quarterly": "stage",
                  "pulse": "pulse", "tickers": "pulse"}[kind]
        data_file = _PROJECT_ROOT / "data" / subdir / f"{file_prefix}_{kind}.json"
        if data_file.exists():
            data_file.unlink()
            log_info(logger, f"Deleted {data_file.name}", step="sync", index=key)

    # Delete logo folder
    logo_dir = _LOGOS_DIR / key
    if logo_dir.exists():
        shutil.rmtree(logo_dir)
        log_info(logger, f"Deleted logo folder {key}/", step="sync", index=key)


# ---------------------------------------------------------------------------
# Logo sync
# ---------------------------------------------------------------------------

def _get_logo_token():
    return os.getenv("LOGO_DEV_TOKEN")


def _get_domain_for_symbol(symbol):
    """Get company domain from yfinance website field."""
    try:
        import yfinance as yf
        info = yf.Ticker(symbol).info
        website = info.get("website", "")
        if website:
            return urlparse(website).netloc.replace("www.", "")
    except Exception:
        pass
    return None


def _download_logo(symbol, domain, token, dest):
    try:
        import requests
        url = f"https://img.logo.dev/{domain}?token={token}&size=128&format=png"
        r = requests.get(url, timeout=10)
        if r.status_code == 200 and len(r.content) > 100:
            dest.write_bytes(r.content)
            return True
    except Exception:
        pass
    return False


def _sync_logos_added(key, added_symbols, token):
    if not token:
        return

    logo_dir = _LOGOS_DIR / key
    logo_dir.mkdir(parents=True, exist_ok=True)

    for symbol in sorted(added_symbols):
        dest = logo_dir / f"{symbol}.png"
        if dest.exists():
            continue

        domain = _get_domain_for_symbol(symbol)
        if not domain:
            log_warning(logger, f"No website found for {symbol} — skipping logo",
                        step="sync", index=key, symbol=symbol)
            continue

        if _download_logo(symbol, domain, token, dest):
            log_info(logger, f"Downloaded logo for {symbol}",
                     step="sync", index=key, symbol=symbol, domain=domain)
        else:
            log_warning(logger, f"Logo download failed for {symbol}",
                        step="sync", index=key, symbol=symbol, domain=domain)

        time.sleep(0.3)


def _sync_logos_removed(key, removed_symbols):
    logo_dir = _LOGOS_DIR / key
    if not logo_dir.exists():
        return

    for symbol in sorted(removed_symbols):
        logo_file = logo_dir / f"{symbol}.png"
        if logo_file.exists():
            logo_file.unlink()
            log_info(logger, f"Deleted logo for {symbol}",
                     step="sync", index=key, symbol=symbol)


def _cleanup_orphan_logos(key, def_symbols):
    """Delete logo files that don't match any symbol in the definition."""
    logo_dir = _LOGOS_DIR / key
    if not logo_dir.exists():
        return

    for logo_file in logo_dir.glob("*.png"):
        symbol = logo_file.stem
        if symbol not in def_symbols:
            logo_file.unlink()
            log_info(logger, f"Deleted orphan logo {symbol}",
                     step="sync", index=key, symbol=symbol)


def download_logos(key, symbols):
    """Public API: download logos for all symbols in an index. Idempotent — skips existing."""
    token = _get_logo_token()
    if not token:
        log_warning(logger, "LOGO_DEV_TOKEN not set — skipping logo download", step="setup", index=key)
        return
    _sync_logos_added(key, symbols, token)


# ---------------------------------------------------------------------------
# Dimension reload
# ---------------------------------------------------------------------------

def _reload_dims(keys_to_reload):
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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run():
    """Full sync: new/removed indices, new/removed symbols, tables, dims, logos."""
    conn = get_connection()
    cursor = conn.cursor()
    keys_to_reload = []
    logo_token = _get_logo_token()

    if not logo_token:
        log_info(logger, "LOGO_DEV_TOKEN not set — logo sync will be skipped",
                 step="sync")

    try:
        total_added = 0
        total_purged = 0
        definition_keys = {idx["key"] for idx in INDICES}

        # --- Detect removed indices (in DB but no definition file) ---
        db_indices = _get_db_indices(cursor)
        removed_indices = db_indices - definition_keys

        for key in sorted(removed_indices):
            _purge_index(cursor, conn, key)
            total_purged += 1

        # --- Upsert index metadata (name, color, currency, etc.) ---
        _upsert_dim_index(cursor, conn)

        # --- Sync each active index ---
        for idx in INDICES:
            key = idx["key"]

            # Ensure OHLCV tables exist (bootstrap for new indices)
            _ensure_ohlcv_tables(cursor, conn, idx)

            def_symbols = _get_definition_symbols(key)
            if def_symbols is None:
                log_warning(logger, "Definition file not found — skipping sync",
                            step="sync", index=key)
                continue

            # Clean up orphan logos (e.g. from previous removals before logo sync existed)
            _cleanup_orphan_logos(key, def_symbols)

            dim_symbols, _ = _get_dim_symbols(key)
            db_symbols = _get_db_symbols(cursor, key)

            # Rebuild dim JSON from DB if missing (e.g. ephemeral GCP container)
            if dim_symbols is None and db_symbols:
                records = _export_db_dims(key)
                if records:
                    dim_p = data_path(key, "dim")
                    dim_p.parent.mkdir(parents=True, exist_ok=True)
                    safe_write_json(dim_p, records)
                    dim_symbols = {r["symbol"] for r in records if "symbol" in r}
                    log_info(logger, f"Rebuilt dim JSON from DB ({len(records)} records)",
                             step="sync", index=key)

            known_symbols = (dim_symbols or set()) | db_symbols
            new_symbols = def_symbols - known_symbols
            removed_symbols = known_symbols - def_symbols

            if not new_symbols and not removed_symbols:
                log_info(logger, "Definitions in sync",
                         step="sync", index=key,
                         def_count=len(def_symbols),
                         dim_count=len(dim_symbols or set()),
                         db_count=len(db_symbols))
                continue

            with StepTimer() as timer:
                # New symbols
                if new_symbols:
                    log_info(logger, f"Detected {len(new_symbols)} new symbol(s)",
                             step="sync", index=key, symbols=sorted(new_symbols))
                    added = _fetch_new_symbols(key, new_symbols)
                    total_added += len(added)
                    if added:
                        keys_to_reload.append(key)
                        _sync_logos_added(key, added, logo_token)

                # Removed symbols
                if removed_symbols:
                    log_warning(logger, f"Detected {len(removed_symbols)} removed symbol(s)",
                                step="sync", index=key, symbols=sorted(removed_symbols))
                    _remove_from_dim_json(key, removed_symbols)
                    _purge_symbols_from_db(cursor, conn, key, removed_symbols)
                    _sync_logos_removed(key, removed_symbols)
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
