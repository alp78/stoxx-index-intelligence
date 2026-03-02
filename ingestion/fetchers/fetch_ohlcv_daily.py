"""Fetches the last 5 trading days of OHLCV data for all stocks in each index.
Designed to run daily. The merge loader handles deduplication against bronze."""

import json
import sys
import yfinance as yf
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import INDICES, data_path


def fetch_recent_ohlcv(reg_file, output_file):
    print(f"\n--- Fetching recent OHLCV from {reg_file} ---")

    # Airflow Compatibility: Ensure output directory exists before saving
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(reg_file, 'r', encoding='utf-8') as f:
            registry = json.load(f)
    except FileNotFoundError:
        print(f"Error: {reg_file} not found. Skipping...")
        return

    # Fetch last 5 calendar days to cover weekends/holidays
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

    all_records = []

    for company in registry:
        symbol = company.get('symbol')
        if not symbol:
            continue

        print(f"Fetching: {symbol:<8} [{start_date} to {end_date}]", end="\r")

        try:
            tkr = yf.Ticker(symbol)
            df = tkr.history(start=start_date, end=end_date, interval="1d", auto_adjust=False, actions=True)

            if df.empty:
                print(f"\nWarning: No data found for {symbol}")
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
                all_records.append(record)

        except Exception as e:
            print(f"\nError fetching {symbol}: {e}")

        time.sleep(0.15)

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_records, f, indent=4, ensure_ascii=False)

    print(f"\nSuccess: Saved {len(all_records)} OHLCV rows to {output_file}")


if __name__ == "__main__":
    for idx in INDICES:
        key = idx["key"]
        fetch_recent_ohlcv(
            reg_file=data_path(key, "dim"),
            output_file=data_path(key, "ohlcv_daily")
        )
