import json
import sys
import yfinance as yf
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import INDICES, data_path

def discover_pulse_tickers(reg_file, output_file):
    """Ranks registered stocks by activity score to find the 10 most dynamic.
    Activity = 0.5 * volume_surge + 0.5 * price_range_intensity (z-scored).
    Intended to run hourly (e.g., via Airflow hourly DAG)."""
    print(f"\n--- Discovering top 10 most active from {reg_file} ---")

    try:
        with open(reg_file, 'r', encoding='utf-8') as f:
            registry = json.load(f)
    except FileNotFoundError:
        print(f"Error: {reg_file} not found.")
        return

    valid_symbols = [c.get('symbol') for c in registry if c.get('symbol')]
    print(f"Found {len(valid_symbols)} registered symbols. Ranking by activity...")

    raw_scores = []
    for symbol in valid_symbols:
        print(f"Ranking: {symbol:<8}", end="\r")
        try:
            tkr = yf.Ticker(symbol)
            info = tkr.info

            last_vol = info.get("volume")
            avg_vol = info.get("averageDailyVolume10Day")
            day_high = info.get("regularMarketDayHigh")
            day_low = info.get("regularMarketDayLow")
            prev_close = info.get("regularMarketPreviousClose")

            vol_surge = (last_vol / avg_vol) if (last_vol and avg_vol) else 0.0
            range_intensity = ((day_high - day_low) / prev_close) if (day_high and day_low and prev_close) else 0.0

            raw_scores.append({
                "symbol": symbol,
                "volumeSurge": round(vol_surge, 4),
                "rangeIntensity": round(range_intensity, 4)
            })
        except Exception as e:
            print(f"\nError ranking {symbol}: {e}")

        time.sleep(0.1)

    if not raw_scores:
        print("No scores collected, skipping.")
        return

    # Z-score both metrics across the index, then composite
    vol_vals = [s['volumeSurge'] for s in raw_scores]
    rng_vals = [s['rangeIntensity'] for s in raw_scores]

    def z_scores(vals):
        n = len(vals)
        mean = sum(vals) / n
        std = (sum((v - mean) ** 2 for v in vals) / n) ** 0.5
        if std == 0:
            return [0.0] * n
        return [(v - mean) / std for v in vals]

    vol_z = z_scores(vol_vals)
    rng_z = z_scores(rng_vals)

    for i, score in enumerate(raw_scores):
        score['volZ'] = round(vol_z[i], 4)
        score['rngZ'] = round(rng_z[i], 4)
        score['activityScore'] = round(0.5 * vol_z[i] + 0.5 * rng_z[i], 4)

    sorted_list = sorted(raw_scores, key=lambda x: x['activityScore'], reverse=True)
    top_10 = sorted_list[:10]

    print(f"\nTop 10 most active:")
    for i, item in enumerate(top_10, 1):
        print(f"  {i:>2}. {item['symbol']:<8}  score={item['activityScore']:>7.3f}  vol_surge={item['volumeSurge']:>6.2f}x  range={item['rangeIntensity']*100:>5.2f}%")

    # Airflow Compatibility: Ensure output directory exists before saving
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    tickers = {
        "discovered_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "symbols": [item['symbol'] for item in top_10],
        "ranking": top_10
    }

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(tickers, f, indent=4, ensure_ascii=False)
    print(f"\nSuccess: Saved pulse tickers to {output_file}")


def fetch_pulse(ticker_file, output_file, index_name):
    """Fetches a lightweight quote snapshot for the pre-discovered tickers.
    Intended to run every minute (e.g., via Airflow minute-level DAG)."""
    try:
        with open(ticker_file, 'r', encoding='utf-8') as f:
            tickers = json.load(f)
    except FileNotFoundError:
        print(f"Error: {ticker_file} not found. Run with --discover first.")
        return

    symbols = tickers.get('symbols', [])
    if not symbols:
        print(f"No tickers in {ticker_file}, skipping.")
        return

    print(f"\n--- Pulse: {index_name} ({len(symbols)} symbols, discovered {tickers['discovered_at']}) ---")

    # Airflow Compatibility: Ensure output directory exists before saving
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    pulse_records = []

    for symbol in symbols:
        print(f"Pulse: {symbol:<8}", end="\r")

        try:
            tkr = yf.Ticker(symbol)
            info = tkr.info

            current_price = info.get("currentPrice")
            prev_close = info.get("regularMarketPreviousClose")
            bid = info.get("bid")
            ask = info.get("ask")
            volume = info.get("volume")
            avg_vol = info.get("averageDailyVolume10Day")

            record = {
                "symbol": symbol,
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "price": {
                    "current": current_price,
                    "open": info.get("regularMarketOpen"),
                    "dayHigh": info.get("regularMarketDayHigh"),
                    "dayLow": info.get("regularMarketDayLow"),
                    "previousClose": prev_close,
                    "change": round(current_price - prev_close, 4) if (current_price and prev_close) else None,
                    "changePct": round((current_price / prev_close - 1) * 100, 4) if (current_price and prev_close) else None
                },
                "book": {
                    "bid": bid,
                    "ask": ask,
                    "bidSize": info.get("bidSize"),
                    "askSize": info.get("askSize"),
                    "spread": round(ask - bid, 4) if (ask and bid) else None
                },
                "volume": {
                    "current": volume,
                    "average10Day": avg_vol,
                    "ratio": round(volume / avg_vol, 4) if (volume and avg_vol) else None
                }
            }
            pulse_records.append(record)
        except Exception as e:
            print(f"\nError fetching {symbol}: {e}")

        time.sleep(0.1)

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(pulse_records, f, indent=4, ensure_ascii=False)
    print(f"\nSuccess: Saved {len(pulse_records)} pulse records to {output_file}")


if __name__ == "__main__":
    if "--discover" in sys.argv:
        # Run hourly: ranks all 50 per index by activity, saves top 10 each
        for idx in INDICES:
            key = idx["key"]
            discover_pulse_tickers(
                reg_file=data_path(key, "dim"),
                output_file=data_path(key, "tickers")
            )
    else:
        # Run every minute: fetches pulse for the pre-discovered tickers
        for idx in INDICES:
            key = idx["key"]
            fetch_pulse(
                ticker_file=data_path(key, "tickers"),
                output_file=data_path(key, "pulse"),
                index_name=idx["name"]
            )
