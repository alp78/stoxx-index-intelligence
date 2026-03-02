"""Fetches full OHLCV history from yfinance for all stocks in each index."""

import json
import sys
import yfinance as yf
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import INDICES, data_path
from logger import get_logger, log_info, log_warning, log_error, StepTimer

logger = get_logger(__name__)


def fetch_time_series_history(reg_file, output_file):
    log_info(logger, "Fetch started", step="fetch", source="yfinance",
             kind="ohlcv_history", reg_file=str(reg_file))

    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(reg_file, 'r', encoding='utf-8') as f:
            registry = json.load(f)
    except FileNotFoundError:
        log_error(logger, "Registry file not found", step="fetch",
                  reg_file=str(reg_file))
        return

    all_history = []
    end_date = datetime.now().strftime('%Y-%m-%d')

    with StepTimer() as timer:
        for company in registry:
            symbol = company.get('symbol')
            start_date = company.get('_price_data_start', '2021-01-01')

            if not symbol:
                continue

            try:
                tkr = yf.Ticker(symbol)
                df = tkr.history(start=start_date, end=end_date, interval="1d",
                                 auto_adjust=False, actions=True)

                if df.empty:
                    log_warning(logger, "No data for symbol", step="fetch",
                                symbol=symbol)
                    continue

                df = df.reset_index()

                for _, row in df.iterrows():
                    raw_close = float(row['Close'])
                    adj_close = float(row.get('Adj Close', raw_close))

                    record = {
                        "symbol": symbol,
                        "date": row['Date'].strftime('%Y-%m-%d'),
                        "open": round(float(row['Open']), 4),
                        "high": round(float(row['High']), 4),
                        "low": round(float(row['Low']), 4),
                        "close": round(raw_close, 4),
                        "adj_close": round(adj_close, 4),
                        "volume": int(row['Volume']),
                        "dividends": float(row.get('Dividends', 0)),
                        "stock_splits": float(row.get('Stock Splits', 0))
                    }
                    all_history.append(record)

            except Exception as e:
                log_error(logger, "Symbol fetch failed", step="fetch",
                          symbol=symbol, error=str(e))

            time.sleep(0.15)

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_history, f, indent=4, ensure_ascii=False)

    log_info(logger, "Fetch complete", step="fetch", kind="ohlcv_history",
             records_fetched=len(all_history), duration_ms=timer.duration_ms)


if __name__ == "__main__":
    for idx in INDICES:
        key = idx["key"]
        fetch_time_series_history(
            reg_file=data_path(key, "dim"),
            output_file=data_path(key, "ohlcv_history")
        )
