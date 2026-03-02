"""Pulse data fetcher: discovers most active tickers and fetches real-time snapshots.

Usage:
  python fetch_pulse.py --discover   # Hourly: rank all stocks, save top 10
  python fetch_pulse.py              # Every minute: fetch pulse for discovered tickers
"""

import json
import sys
import yfinance as yf
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from utils.config import INDICES, data_path, safe_write_json, cet_now_str
from utils.db import get_connection
from utils.logger import get_logger, log_info, log_warning, log_error, StepTimer

logger = get_logger(__name__)


def _get_tickers_from_db(index_key):
    """Read pulse ticker symbols from bronze.pulse_tickers (DB fallback for Cloud Run)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT symbol FROM bronze.pulse_tickers WHERE _index = ?", index_key)
    symbols = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return symbols


def _get_registry_from_db(index_key):
    """Read stock symbols from bronze.index_dim (DB fallback for Cloud Run)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT symbol FROM bronze.index_dim WHERE _index = ?", index_key)
    symbols = [row[0] for row in cursor.fetchall() if row[0]]
    cursor.close()
    conn.close()
    return symbols


def discover_pulse_tickers(reg_file, output_file, index_key=None):
    """Ranks registered stocks by activity score to find the 10 most dynamic.
    Activity = 0.5 * volume_surge + 0.5 * price_range_intensity (z-scored).
    Falls back to reading symbols from bronze.index_dim when the registry
    JSON file is missing (Cloud Run ephemeral filesystem).
    Intended to run hourly."""
    log_info(logger, "Ranking all stocks by activity score to find top 10 most dynamic tickers",
             step="fetch", kind="pulse_discover", reg_file=str(reg_file))

    valid_symbols = []
    try:
        with open(reg_file, 'r', encoding='utf-8') as f:
            registry = json.load(f)
        valid_symbols = [c.get('symbol') for c in registry if c.get('symbol')]
    except FileNotFoundError:
        if index_key:
            log_info(logger, "Registry JSON not found — falling back to bronze.index_dim",
                     step="fetch", index=index_key)
            valid_symbols = _get_registry_from_db(index_key)

    if not valid_symbols:
        log_warning(logger, "Cannot discover pulse tickers — no stock symbols available (file or DB)",
                    step="fetch", reg_file=str(reg_file), index=index_key)
        return

    raw_scores = []
    with StepTimer() as timer:
        for symbol in valid_symbols:
            try:
                tkr = yf.Ticker(symbol)
                info = tkr.info

                last_vol = info.get("volume")
                avg_vol = info.get("averageDailyVolume10Day")
                day_high = info.get("regularMarketDayHigh")
                day_low = info.get("regularMarketDayLow")
                prev_close = info.get("regularMarketPreviousClose")

                vol_surge = (last_vol / avg_vol) if (last_vol is not None and avg_vol) else 0.0
                range_intensity = ((day_high - day_low) / prev_close) if (day_high is not None and day_low is not None and prev_close) else 0.0

                raw_scores.append({
                    "symbol": symbol,
                    "volumeSurge": round(vol_surge, 4),
                    "rangeIntensity": round(range_intensity, 4)
                })
            except Exception as e:
                log_error(logger, "Failed to compute activity score for symbol",
                          step="fetch", symbol=symbol, error=str(e))

            time.sleep(0.1)

    if not raw_scores:
        log_warning(logger, "No activity scores collected — all symbols failed or returned no data",
                    step="fetch", kind="pulse_discover")
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

    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    tickers = {
        "discovered_at": cet_now_str(),
        "symbols": [item['symbol'] for item in top_10],
        "ranking": top_10
    }

    safe_write_json(output_file, tickers)

    log_info(logger, "Pulse ticker discovery complete — top 10 tickers ranked by activity score",
             step="fetch", kind="pulse_discover",
             symbols_ranked=len(raw_scores), top_10=[t['symbol'] for t in top_10],
             duration_ms=timer.duration_ms)


def fetch_pulse(ticker_file, output_file, index_name, index_key=None):
    """Fetches a lightweight quote snapshot for the pre-discovered tickers.
    Falls back to reading symbols from bronze.pulse_tickers when the JSON
    file is missing (Cloud Run ephemeral filesystem).
    Intended to run every 5 minutes."""
    symbols = []
    try:
        with open(ticker_file, 'r', encoding='utf-8') as f:
            tickers = json.load(f)
        symbols = tickers.get('symbols', [])
    except FileNotFoundError:
        if index_key:
            log_info(logger, "Ticker JSON not found — falling back to bronze.pulse_tickers",
                     step="fetch", index=index_key)
            symbols = _get_tickers_from_db(index_key)

    if not symbols:
        log_warning(logger, "Cannot fetch pulse — no ticker symbols available (file or DB)",
                    step="fetch", ticker_file=str(ticker_file),
                    index=index_key or index_name)
        return

    log_info(logger, "Fetching real-time price/book/volume snapshots for pulse tickers",
             step="fetch", kind="pulse", index=index_name, symbols=len(symbols))

    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    pulse_records = []

    with StepTimer() as timer:
        for symbol in symbols:
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
                    "timestamp": cet_now_str(),
                    "price": {
                        "current": current_price,
                        "open": info.get("regularMarketOpen"),
                        "dayHigh": info.get("regularMarketDayHigh"),
                        "dayLow": info.get("regularMarketDayLow"),
                        "previousClose": prev_close,
                        "change": round(current_price - prev_close, 4) if (current_price is not None and prev_close is not None) else None,
                        "changePct": round((current_price / prev_close - 1) * 100, 4) if (current_price is not None and prev_close) else None
                    },
                    "book": {
                        "bid": bid,
                        "ask": ask,
                        "bidSize": info.get("bidSize"),
                        "askSize": info.get("askSize"),
                        "spread": round(ask - bid, 4) if (ask is not None and bid is not None) else None
                    },
                    "volume": {
                        "current": volume,
                        "average10Day": avg_vol,
                        "ratio": round(volume / avg_vol, 4) if (volume is not None and avg_vol) else None
                    }
                }
                pulse_records.append(record)
            except Exception as e:
                log_error(logger, "Failed to fetch pulse snapshot for symbol",
                          step="fetch", symbol=symbol, error=str(e))

            time.sleep(0.1)

    safe_write_json(output_file, pulse_records)

    log_info(logger, "Pulse snapshot fetch complete — wrote real-time quotes to JSON",
             step="fetch", kind="pulse", index=index_name,
             records_fetched=len(pulse_records), duration_ms=timer.duration_ms)


if __name__ == "__main__":
    if "--discover" in sys.argv:
        for idx in INDICES:
            key = idx["key"]
            discover_pulse_tickers(
                reg_file=data_path(key, "dim"),
                output_file=data_path(key, "tickers"),
                index_key=key
            )
    else:
        for idx in INDICES:
            key = idx["key"]
            fetch_pulse(
                ticker_file=data_path(key, "tickers"),
                output_file=data_path(key, "pulse"),
                index_name=idx["name"],
                index_key=key
            )
