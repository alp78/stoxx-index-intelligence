import json
import yfinance as yf
import time
from datetime import datetime
from pathlib import Path

def fetch_daily_signals(reg_file, output_file):
    print(f"\n--- Processing {reg_file} ---")
    
    # Airflow Compatibility: Ensure output directory exists before saving
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    
    daily_signals = []
    
    try:
        with open(reg_file, 'r', encoding='utf-8') as f:
            registry = json.load(f)
    except FileNotFoundError:
        print(f"Error: {reg_file} not found. Skipping...")
        return

    for company in registry:
        # Robust symbol lookup for flattened registry
        symbol = company.get('symbol')
        
        if not symbol:
            print(f"Skipping record in {reg_file}: 'symbol' key not found.")
            continue

        print(f"Fetching Daily Signals: {symbol:<8}", end="\r")
        
        try:
            tkr = yf.Ticker(symbol)
            info = tkr.info
            
            current_price = info.get("currentPrice")
            high_52w = info.get("fiftyTwoWeekHigh")
            target_price = info.get("targetMedianPrice")
            
            record = {
                "symbol": symbol,
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "price_metrics": {
                    "currentPrice": current_price,
                    "forwardPE": info.get("forwardPE"),
                    "priceToBook": info.get("priceToBook"),
                    "evToEbitda": info.get("enterpriseToEbitda"),
                    "dividendYield": info.get("dividendYield"),
                    "marketCap": info.get("marketCap"),
                    "beta": info.get("beta")
                },
                "market_context": {
                    "fiftyTwoWeekChange": info.get("52WeekChange"),
                    "SandP52WeekChange": info.get("SandP52WeekChange")
                },
                "momentum_signals": {
                    "fiftyDayAverage": info.get("fiftyDayAverage"),
                    "twoHundredDayAverage": info.get("twoHundredDayAverage"),
                    "distFrom52WeekHigh": (1 - (current_price / high_52w)) if (current_price and high_52w) else None
                },
                "sentiment_signals": {
                    "targetMedianPrice": target_price,
                    "recommendationMean": info.get("recommendationMean"),
                    "upsidePotential": (target_price / current_price - 1) if (current_price and target_price) else None
                }
            }
            daily_signals.append(record)
        except Exception as e:
            print(f"\nError fetching {symbol}: {e}")
        
        time.sleep(0.15)

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(daily_signals, f, indent=4, ensure_ascii=False)
    print(f"\nSuccess: Saved {len(daily_signals)} signal records to {output_file}")

if __name__ == "__main__":
    # Resolve absolute paths based on this script's location
    # Assumes script is in /ingestion/fetchers, going up two levels to /data
    script_dir = Path(__file__).resolve().parent
    stage_dir = script_dir.parent.parent / "data" / "stage"
    signals_dir = script_dir.parent.parent / "data" / "signals"
    
    # Track 2: Market Signals - Euro Index
    fetch_daily_signals(
        reg_file=stage_dir / "euro_stoxx_50.json", 
        output_file=signals_dir / "euro_stoxx_50_daily_signals.json"
    )
    
    # Track 2: Market Signals - USA Index
    fetch_daily_signals(
        reg_file=stage_dir / "stoxx_usa_50.json", 
        output_file=signals_dir / "stoxx_usa_50_daily_signals.json"
    )