import json
import yfinance as yf
import time
from datetime import datetime, timedelta
from pathlib import Path

def format_epoch(epoch_val):
    """Safely converts Unix timestamps to YYYY-MM-DD, bypassing Windows OS limits."""
    if epoch_val is None: 
        return None
    try:
        # Standard Unix epoch start
        epoch_dt = datetime(1970, 1, 1)
        return (epoch_dt + timedelta(seconds=float(epoch_val))).strftime('%Y-%m-%d')
    except Exception: 
        return None

def fetch_quarterly_fundamentals(reg_file, output_file):
    print(f"\n--- Processing {reg_file} ---")
    
    # Airflow Compatibility: Ensure output directory exists before saving
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    
    all_fundamentals = []
    
    try:
        with open(reg_file, 'r', encoding='utf-8') as f:
            registry = json.load(f)
    except FileNotFoundError:
        print(f"Error: {reg_file} not found. Skipping...")
        return

    for company in registry:
        # Robust symbol lookup (handles flattened or nested)
        symbol = company.get('symbol') or company.get('static', {}).get('symbol')
        
        if not symbol:
            print(f"Skipping record in {reg_file}: 'symbol' key not found.")
            continue

        print(f"Fetching Fundamentals: {symbol:<8}", end="\r")
        
        try:
            tkr = yf.Ticker(symbol)
            info = tkr.info
            
            # Accounting and Governance fields (Slow-changing)
            record = {
                "symbol": symbol,
                "as_of_date": datetime.now().strftime('%Y-%m-%d'),
                "quality_metrics": {
                    "grossMargins": info.get("grossMargins"),
                    "operatingMargins": info.get("operatingMargins"),
                    "returnOnEquity": info.get("returnOnEquity"),
                    "revenueGrowth": info.get("revenueGrowth"),
                    "earningsGrowth": info.get("earningsGrowth")
                },
                "capital_structure": {
                    "sharesOutstanding": info.get("sharesOutstanding"),
                    "floatShares": info.get("floatShares") or info.get("sharesOutstanding"),
                    "debtToEquity": info.get("debtToEquity"),
                    "currentRatio": info.get("currentRatio"),
                    "freeCashflow": info.get("freeCashflow")
                },
                "fiscal_calendar": {
                    "_last_fiscal_year_end": format_epoch(info.get("lastFiscalYearEnd")),
                    "_most_recent_quarter": format_epoch(info.get("mostRecentQuarter"))
                },
                "governance": {
                    "overallRisk": info.get("overallRisk"),
                    "auditRisk": info.get("auditRisk"),
                    "boardRisk": info.get("boardRisk"),
                    "compensationRisk": info.get("compensationRisk"),
                    "shareHolderRightsRisk": info.get("shareHolderRightsRisk"),
                    "esgPopulated": True
                }
            }
            all_fundamentals.append(record)
        except Exception as e:
            print(f"\nError fetching {symbol}: {e}")
        
        time.sleep(0.15) # Rate limiting respect

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_fundamentals, f, indent=4, ensure_ascii=False)
    print(f"\nSuccess: Saved {len(all_fundamentals)} records to {output_file}")

if __name__ == "__main__":
    # Resolve absolute paths based on this script's location
    script_dir = Path(__file__).resolve().parent
    stage_dir = script_dir.parent / "data" / "stage"
    signals_dir = script_dir.parent / "data" / "signals"
    
    # Process Euro Stoxx
    fetch_quarterly_fundamentals(
        reg_file=stage_dir / "euro_stoxx_50.json", 
        output_file=signals_dir / "euro_stoxx_50_quarter_signals.json"
    )
    
    # Process USA Top 50
    fetch_quarterly_fundamentals(
        reg_file=stage_dir / "stoxx_usa_50.json", 
        output_file=signals_dir / "stoxx_usa_50_quarter_signals.json"
    )