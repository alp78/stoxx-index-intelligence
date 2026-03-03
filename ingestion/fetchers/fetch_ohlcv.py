"""Smart OHLCV fetcher with gap detection.

Queries silver for the last available date per symbol, determines "today"
per exchange timezone, and fetches only missing data from yfinance.

Scenarios:
  - First run (no silver data): full history from _price_data_start
  - Normal run: fetch from last_silver_date (1-day overlap for safety)
  - Gap recovery: same as normal — fetches entire missing range
  - Up to date: skip symbol entirely
"""

import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import yfinance as yf

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from utils.db import get_connection, get_index_symbols
from utils.config import get_index, data_path, silver_ohlcv, safe_write_json
from utils.logger import get_logger, log_info, log_warning, log_error, StepTimer

logger = get_logger(__name__)

_DEFAULT_HISTORY_START = "2021-01-01"


def fetch_ohlcv(index_key):
    idx = get_index(index_key)
    output_file = data_path(index_key, "ohlcv")

    log_info(logger, "Fetching OHLCV prices from yfinance (gap-aware)",
             step="fetch", source="yfinance", kind="ohlcv", index=index_key)

    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    # Read symbol list from bronze.index_dim (populated by setup_index.py)
    rows = get_index_symbols(index_key)
    if not rows:
        log_error(logger, "No symbols in bronze.index_dim — run setup_index.py first",
                  step="fetch", index=index_key)
        return
    registry = [{"symbol": r[0], "_price_data_start": r[1]} for r in rows]

    # Query silver for last date per symbol
    conn = get_connection()
    cursor = conn.cursor()

    silver_table = silver_ohlcv(index_key)
    last_dates = {}
    try:
        cursor.execute(f"""
            SELECT symbol, CONVERT(VARCHAR(10), MAX(date), 120)
            FROM {silver_table}
            WHERE is_filled = 0
            GROUP BY symbol
        """)
        last_dates = {r[0]: r[1] for r in cursor.fetchall()}
    except Exception:
        # Table might not exist on first run — will do full history fetch
        log_warning(logger, "Silver OHLCV table not found — will fetch full history",
                    step="fetch", index=index_key, table=silver_table)

    # Get exchange timezone per symbol from bronze.index_dim
    try:
        cursor.execute("""
            SELECT symbol, exchange_timezone_name
            FROM bronze.index_dim
            WHERE _index = ?
        """, index_key)
        tz_map = {r[0]: r[1] for r in cursor.fetchall()}
    except Exception:
        tz_map = {}
        log_warning(logger, "Could not read exchange timezones — defaulting to UTC",
                    step="fetch", index=index_key)
    finally:
        cursor.close()
        conn.close()

    all_records = []
    fetched = 0
    skipped = 0
    failed = 0

    with StepTimer() as timer:
        for company in registry:
            symbol = company.get("symbol")
            if not symbol:
                continue

            # Determine "today" in the symbol's exchange timezone
            tz_name = tz_map.get(symbol, "UTC")
            try:
                today = datetime.now(ZoneInfo(tz_name)).strftime("%Y-%m-%d")
            except Exception:
                today = datetime.now(ZoneInfo("UTC")).strftime("%Y-%m-%d")

            last_silver = last_dates.get(symbol)

            if last_silver and last_silver >= today:
                skipped += 1
                continue

            # Determine start date
            if last_silver:
                start_date = last_silver  # 1-day overlap for safety
            else:
                start_date = company.get("_price_data_start", _DEFAULT_HISTORY_START)

            try:
                tkr = yf.Ticker(symbol)
                df = tkr.history(start=start_date, end=today, interval="1d",
                                 auto_adjust=False, actions=True)

                if df.empty:
                    log_warning(logger, "yfinance returned no OHLCV rows for symbol",
                                step="fetch", symbol=symbol, start=start_date, end=today)
                    continue

                df = df.reset_index()

                for _, row in df.iterrows():
                    raw_close = float(row["Close"])
                    adj_close = float(row.get("Adj Close", raw_close))

                    all_records.append({
                        "symbol": symbol,
                        "date": row["Date"].strftime("%Y-%m-%d"),
                        "open": round(float(row["Open"]), 4),
                        "high": round(float(row["High"]), 4),
                        "low": round(float(row["Low"]), 4),
                        "close": round(raw_close, 4),
                        "adj_close": round(adj_close, 4),
                        "volume": int(row["Volume"]),
                        "dividends": float(row.get("Dividends", 0)),
                        "stock_splits": float(row.get("Stock Splits", 0)),
                    })

                fetched += 1

            except Exception as e:
                log_error(logger, "Failed to fetch OHLCV for symbol from yfinance",
                          step="fetch", symbol=symbol, error=str(e))
                failed += 1

            time.sleep(0.15)

    safe_write_json(output_file, all_records)

    log_info(logger, "OHLCV fetch complete — wrote JSON with gap-fill data",
             step="fetch", kind="ohlcv", index=index_key,
             records_fetched=len(all_records), symbols_fetched=fetched,
             symbols_skipped_uptodate=skipped, symbols_failed=failed,
             duration_ms=timer.duration_ms)


if __name__ == "__main__":
    from utils.config import get_all_keys
    for key in get_all_keys():
        fetch_ohlcv(key)
