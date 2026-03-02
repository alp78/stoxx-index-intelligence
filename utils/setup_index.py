"""Sets up a new index: creates tables, fetches dims, and populates all data.

After setup, bronze AND silver are fully populated:
  - OHLCV tables (full history in silver, latest day in bronze)
  - Signals daily & quarterly (current snapshot in bronze, initial row in silver)
  - Pulse tickers & pulse (current snapshot in bronze)
  - Trading calendar and SCD2 index dimensions

Idempotent: skips tables that already exist, skips dim fetch if file exists.

Usage:
  python utils/setup_index.py euro_stoxx_50       # Setup one index
  python utils/setup_index.py                     # Setup all indices
"""

import re
import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.db import get_connection
from config import INDICES, data_path, bronze_ohlcv
from logger import get_logger, log_info, log_warning, log_error, StepTimer

logger = get_logger(__name__)

_DEFINITIONS_DIR = Path(__file__).resolve().parent.parent / "data" / "definitions"


BRONZE_OHLCV_DDL = """
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

    CREATE INDEX IX_bronze_{table}_symbol_date
        ON bronze.{table} (symbol, date);

    PRINT 'Created bronze.{table}';
END
ELSE
    PRINT 'bronze.{table} already exists';
"""

SILVER_OHLCV_DDL = """
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

    CREATE UNIQUE INDEX IX_silver_{table}_symbol_date
        ON silver.{table} (symbol, date);

    PRINT 'Created silver.{table}';
END
ELSE
    PRINT 'silver.{table} already exists';
"""


def _get_available_definitions():
    """List available definition files."""
    if not _DEFINITIONS_DIR.is_dir():
        return []
    return sorted(f.stem for f in _DEFINITIONS_DIR.glob("*.json"))


_SQL_RESERVED = {
    "add", "all", "alter", "and", "any", "as", "asc", "backup", "between",
    "by", "case", "check", "column", "constraint", "create", "database",
    "default", "delete", "desc", "distinct", "drop", "exec", "exists",
    "foreign", "from", "group", "having", "in", "index", "inner", "insert",
    "into", "is", "join", "key", "left", "like", "not", "null", "on", "or",
    "order", "outer", "primary", "select", "set", "table", "top", "truncate",
    "union", "unique", "update", "values", "view", "where",
}


def _validate_key(key):
    """Validate index key format and check definition file exists. Exit with guidance if not."""
    errors = []

    if not key:
        errors.append("key is empty")
    else:
        if not re.match(r'^[a-z]', key):
            errors.append("must start with a lowercase letter (a-z)")
        if not re.match(r'^[a-z][a-z0-9_]*$', key):
            bad = set(re.findall(r'[^a-z0-9_]', key))
            if bad:
                errors.append(f"illegal characters: {', '.join(repr(c) for c in sorted(bad))}")
            if re.match(r'^[0-9]', key):
                errors.append("starts with a digit")
            if key != key.lower():
                errors.append("uppercase letters not allowed")
        if '__' in key:
            errors.append("consecutive underscores not allowed")
        if key.startswith('_') or key.endswith('_'):
            errors.append("must not start or end with an underscore")
        if len(key) > 50:
            errors.append(f"too long ({len(key)} chars, max 50)")
        prefix = key.replace("_", "")
        if prefix in _SQL_RESERVED or key in _SQL_RESERVED:
            errors.append(f"conflicts with SQL reserved word")

    if errors:
        log_error(logger, f"Invalid index key '{key}': {'; '.join(errors)}",
                  step="setup", index=key, errors=errors)
        _log_naming_rules()
        sys.exit(1)

    def_file = _DEFINITIONS_DIR / f"{key}.json"
    if def_file.exists():
        return True

    log_error(logger, f"No definition file found for '{key}'",
              step="setup", index=key, expected=str(def_file))

    available = _get_available_definitions()
    if available:
        log_info(logger, f"Available definitions: {', '.join(available)}",
                 step="setup")
    else:
        log_warning(logger, f"No definition files found in {_DEFINITIONS_DIR}",
                    step="setup")

    _log_naming_rules()
    sys.exit(1)


def _log_naming_rules():
    """Log index key naming rules and definition file template."""
    log_info(logger,
             "Index key naming rules:\n"
             "  - Lowercase letters (a-z), digits (0-9), and underscores only\n"
             "  - Must start with a letter, not a digit or underscore\n"
             "  - Must not end with an underscore\n"
             "  - No consecutive underscores (e.g. my__index)\n"
             "  - No spaces or special characters\n"
             "  - Max 50 characters\n"
             "  - Must not be a SQL reserved word\n"
             f"    ({', '.join(sorted(_SQL_RESERVED))})\n"
             "\n"
             "  The filename becomes the key. Underscores are stripped to derive\n"
             "  the table name (e.g. my_tech_10 -> mytech10_ohlcv).\n"
             "\n"
             "  Create: data/definitions/<key>.json\n"
             "\n"
             '    {\n'
             '        "name": "My Tech 10",\n'
             '        "symbols": ["AAPL", "MSFT", "GOOGL"]\n'
             '    }',
             step="setup")


def _table_exists(cursor, schema, table):
    """Check if a table exists in the given schema."""
    cursor.execute(
        "SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id "
        "WHERE s.name = ? AND t.name = ?", schema, table)
    return cursor.fetchone() is not None


def _create_tables(targets):
    """Create OHLCV tables (bronze + silver) for the given indices."""
    conn = get_connection()
    cursor = conn.cursor()
    created = 0

    for idx in targets:
        index_key = idx["key"]
        table = idx["ohlcv_table"]

        for schema, ddl in [("bronze", BRONZE_OHLCV_DDL), ("silver", SILVER_OHLCV_DDL)]:
            if _table_exists(cursor, schema, table):
                log_info(logger, "OHLCV table already exists — skipping creation",
                         step="setup", index=index_key, schema=schema, table=table)
            else:
                cursor.execute(ddl.format(table=table))
                conn.commit()
                created += 1
                log_info(logger, "Created OHLCV table",
                         step="setup", index=index_key, schema=schema, table=table)

    cursor.close()
    conn.close()
    return created


def _fetch_dims(targets):
    """Fetch dimension data from yfinance for indices that don't have it yet."""
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ingestion"))
    from fetchers.fetch_index_dim import build_registry  # type: ignore[import-not-found]

    fetched = 0
    for idx in targets:
        key = idx["key"]
        dim_file = data_path(key, "dim")
        if dim_file.exists():
            log_info(logger, "Dimension file already exists — skipping yfinance enrichment",
                     step="setup", index=key)
            continue
        try:
            build_registry(key)
            fetched += 1
        except Exception as e:
            log_warning(logger, f"Failed to fetch dimensions from yfinance: {e}",
                        step="setup", index=key)
    return fetched


def _ingest_dims(targets):
    """Load dims into bronze, refresh trading calendar, run SCD2 transform."""
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ingestion"))
    from loaders.load_index_dim import load as load_dim  # type: ignore[import-not-found]
    from transforms.transform_trading_calendar import run as refresh_calendar  # type: ignore[import-not-found]
    from transforms.transform_index_dim import run as transform_dim  # type: ignore[import-not-found]

    for idx in targets:
        key = idx["key"]
        dim_file = data_path(key, "dim")
        if not dim_file.exists():
            log_warning(logger, "No dimension file available — skipping bronze dim load",
                        step="setup", index=key)
            continue
        try:
            load_dim(dim_file, key)
        except Exception as e:
            log_error(logger, f"Failed to load dimensions into bronze: {e}", step="setup", index=key)

    try:
        refresh_calendar()
    except Exception as e:
        log_error(logger, f"Failed to refresh trading calendar: {e}", step="setup")

    try:
        transform_dim()
    except Exception as e:
        log_error(logger, f"Failed to run SCD2 dimension transform: {e}", step="setup")


def _populate_data(targets):
    """Fetch, load OHLCV + signals + pulse for each target index."""
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ingestion"))
    from fetchers.fetch_ohlcv import fetch_ohlcv  # type: ignore[import-not-found]
    from loaders.load_ohlcv import load as load_ohlcv  # type: ignore[import-not-found]
    from fetchers.fetch_signals_daily import fetch_daily_signals  # type: ignore[import-not-found]
    from loaders.load_signals_daily import load as load_signals_daily  # type: ignore[import-not-found]
    from fetchers.fetch_signals_quarterly import fetch_quarterly_fundamentals  # type: ignore[import-not-found]
    from loaders.load_signals_quarterly import load as load_signals_quarterly  # type: ignore[import-not-found]
    from fetchers.fetch_pulse import discover_pulse_tickers, fetch_pulse  # type: ignore[import-not-found]
    from loaders.load_pulse_tickers import load as load_pulse_tickers  # type: ignore[import-not-found]
    from loaders.load_pulse import load as load_pulse  # type: ignore[import-not-found]

    for idx in targets:
        key = idx["key"]
        dim = data_path(key, "dim")
        if not dim.exists():
            log_warning(logger, "No dimension file — cannot populate data for this index",
                        step="setup", index=key)
            continue

        log_info(logger, "Populating all data layers (OHLCV, signals, pulse) for this index",
                 step="setup", index=key)

        # OHLCV
        try:
            fetch_ohlcv(key)
            ohlcv_file = data_path(key, "ohlcv")
            if ohlcv_file.exists():
                load_ohlcv(ohlcv_file, bronze_ohlcv(key))
        except Exception as e:
            log_error(logger, f"Failed to fetch/load OHLCV data: {e}",
                      step="setup", index=key)

        # Signals daily
        try:
            fetch_daily_signals(dim, data_path(key, "signals_daily"))
            sd = data_path(key, "signals_daily")
            if sd.exists():
                load_signals_daily(sd, key)
        except Exception as e:
            log_error(logger, f"Failed to fetch/load daily signals: {e}",
                      step="setup", index=key)

        # Signals quarterly
        try:
            fetch_quarterly_fundamentals(dim, data_path(key, "signals_quarterly"))
            sq = data_path(key, "signals_quarterly")
            if sq.exists():
                load_signals_quarterly(sq, key)
        except Exception as e:
            log_error(logger, f"Failed to fetch/load quarterly signals: {e}",
                      step="setup", index=key)

        # Pulse tickers
        try:
            discover_pulse_tickers(dim, data_path(key, "tickers"))
            t = data_path(key, "tickers")
            if t.exists():
                load_pulse_tickers(t, key)
        except Exception as e:
            log_error(logger, f"Failed to discover/load pulse tickers: {e}",
                      step="setup", index=key)

        # Pulse
        try:
            t = data_path(key, "tickers")
            if t.exists():
                fetch_pulse(t, data_path(key, "pulse"), idx["name"])
                p = data_path(key, "pulse")
                if p.exists():
                    load_pulse(p, key)
        except Exception as e:
            log_error(logger, f"Failed to fetch/load pulse snapshots: {e}",
                      step="setup", index=key)

        log_info(logger, "All data layers populated for this index", step="setup", index=key)


def _run_transforms():
    """Run cross-index transforms: silver (OHLCV, signals) then gold (scores, performance)."""
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ingestion"))
    from transforms.transform_ohlcv import run as transform_ohlcv  # type: ignore[import-not-found]
    from transforms.transform_signals_daily import run as transform_sd  # type: ignore[import-not-found]
    from transforms.transform_signals_quarterly import run as transform_sq  # type: ignore[import-not-found]
    from transforms.transform_scores_daily import run as gold_sd  # type: ignore[import-not-found]
    from transforms.transform_scores_quarterly import run as gold_sq  # type: ignore[import-not-found]
    from transforms.transform_index_performance import run as gold_perf  # type: ignore[import-not-found]

    for name, fn in [("OHLCV", transform_ohlcv),
                     ("signals_daily", transform_sd),
                     ("signals_quarterly", transform_sq),
                     ("scores_daily (gold)", gold_sd),
                     ("scores_quarterly (gold)", gold_sq),
                     ("index_performance (gold)", gold_perf)]:
        try:
            fn()
        except Exception as e:
            log_error(logger, f"Post-setup {name} transform failed: {e}", step="setup")


def setup(key=None):
    """Set up a new index: create tables, fetch dims, populate all data."""
    if key:
        _validate_key(key)
        targets = [idx for idx in INDICES if idx["key"] == key]
        if not targets:
            log_error(logger, "Index key exists in definitions but not in loaded config",
                      step="setup", index=key)
            sys.exit(1)
    else:
        targets = INDICES

    log_info(logger, "Starting index setup — tables, dims, data population, and transforms",
             step="setup", indices=len(targets))

    with StepTimer() as timer:
        _create_tables(targets)
        _fetch_dims(targets)
        _ingest_dims(targets)
        _populate_data(targets)
        _run_transforms()

    log_info(logger, "Index setup complete — all bronze, silver, and gold layers populated",
             step="setup", indices=len(targets), duration_ms=timer.duration_ms)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create OHLCV tables for an index")
    parser.add_argument("index_key", nargs="?", default=None,
                        help="Index key (e.g. euro_stoxx_50). Omit to setup all.")
    args = parser.parse_args()
    setup(args.index_key)
