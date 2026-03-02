import json
import yfinance as yf
import time
from datetime import datetime
from pathlib import Path

def fetch_time_series_history(reg_file, output_file):
    print(f"\n--- Processing History for {reg_file} ---")
    
    # Airflow Compatibility: Ensure output directory exists before saving
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    
    try:
        with open(reg_file, 'r', encoding='utf-8') as f:
            registry = json.load(f)
    except FileNotFoundError:
        print(f"Error: {reg_file} not found. Skipping...")
        return

    all_history = []
    end_date = datetime.now().strftime('%Y-%m-%d')

    for company in registry:
        symbol = company.get('symbol')
        start_date = company.get('_price_data_start', '2021-01-01')
        
        if not symbol:
            continue

        print(f"Downloading History: {symbol:<8} [{start_date} to {end_date}]", end="\r")
        
        try:
            tkr = yf.Ticker(symbol)
            # FIX: Set auto_adjust=False and actions=True to get raw prices + adjustment data
            df = tkr.history(start=start_date, end=end_date, interval="1d", auto_adjust=False, actions=True)
            
            if df.empty:
                print(f"\nWarning: No data found for {symbol}")
                continue

            df = df.reset_index()
            
            for _, row in df.iterrows():
                # 'Close' is now the Raw price
                # 'Adj Close' is the dividend/split adjusted price
                # If 'Adj Close' is missing in the df, we fall back to 'Close'
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
            print(f"\nError fetching {symbol}: {e}")
        
        time.sleep(0.15)

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_history, f, indent=4, ensure_ascii=False)
        
    print(f"\nSuccess: Saved {len(all_history)} daily price rows to {output_file}")

if __name__ == "__main__":
    # Resolve absolute paths based on this script's location
    # Assumes script is in /ingestion/fetchers, going up two levels to /data
    script_dir = Path(__file__).resolve().parent
    stage_dir = script_dir.parent.parent / "data" / "stage"
    history_dir = script_dir.parent.parent / "data" / "history"
    
    # Process Euro Index
    fetch_time_series_history(
        reg_file=stage_dir / "eurostoxx50_dim.json",
        output_file=history_dir / "eurostoxx50_ohlcv_history.json"
    )

    # Process USA Index
    fetch_time_series_history(
        reg_file=stage_dir / "stoxxusa50_dim.json",
        output_file=history_dir / "stoxxusa50_ohlcv_history.json"
    )