"""Fetches index dimension data from Wikipedia + yfinance.

This is a UTILITY script, not part of the automated pipeline.
The pipeline starts from a well-formed {prefix}_dim.json in data/stage/.
This script is one way to produce that file -- the user can use any source.

Each record in the output JSON must have at minimum:
  symbol, exchange, exchangeTimezoneName, _price_data_start
See data/stage/eurostoxx50_dim.json for the full schema.
"""

import pandas as pd
import requests
import yfinance as yf
import json
import sys
import time
import re
from io import StringIO
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import INDICES, data_path

# Wiki sources -- only indices with a known Wikipedia table go here.
# Indices without a wiki source must have their dim.json provided manually.
WIKI_SOURCES = {
    "euro_stoxx": {
        "url": "https://en.wikipedia.org/wiki/EURO_STOXX_50",
        "match_col": "Ticker",
        "dot_to_dash": False,
    },
    "stoxx_usa": {
        "url": "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        "match_col": "Symbol",
        "dot_to_dash": True,
    },
}

def fetch_html_safely(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.text

def clean_company_name(raw_name):
    if not raw_name: return None
    cleaned = re.sub(r'\s{2,}.*', '', str(raw_name))
    return cleaned.strip()

def format_epoch(epoch_val, is_ms=False):
    if epoch_val is None: return None
    try:
        sec = epoch_val / 1000.0 if is_ms else float(epoch_val)
        epoch_dt = datetime(1970, 1, 1)
        return (epoch_dt + timedelta(seconds=sec)).strftime('%Y-%m-%d')
    except: return None

def get_top_50_symbols(url, match_col, index_name, dot_to_dash=False):
    """Ranks the source list by Market Cap to find the top 50 candidates."""
    print(f"\nRanking {index_name} for top 50...")
    html = fetch_html_safely(url)
    df = pd.read_html(StringIO(html), match=match_col)[0]
    raw_tickers = df[match_col].unique().tolist()

    ticker_mcap_map = []
    for raw_tkr in raw_tickers:
        clean_tkr = str(raw_tkr).strip().replace(".", "-") if dot_to_dash else str(raw_tkr).strip()
        print(f"Ranking: {clean_tkr:<8}", end="\r")
        try:
            # fast_info is used for speed during the 500-ticker ranking phase
            tkr = yf.Ticker(clean_tkr)
            mcap = tkr.fast_info.market_cap
            if mcap: ticker_mcap_map.append({"symbol": clean_tkr, "mcap": mcap})
        except: continue
            
    sorted_list = sorted(ticker_mcap_map, key=lambda x: x['mcap'], reverse=True)
    return [item['symbol'] for item in sorted_list[:50]]

def extract_full_identity(symbol, history_start="2021-01-01"):
    """Extracts exactly the fields you requested into a flat dictionary."""
    print(f"Deep Scrape: {symbol:<8}", end="\r")
    try:
        tkr = yf.Ticker(symbol)
        info = tkr.info

        # Windows-safe date conversion
        _range_start = format_epoch(info.get("firstTradeDateMilliseconds"), is_ms=True)

        # Hard Gate: Ensure history exists before our cutoff
        if not _range_start or _range_start > history_start:
            return None

        return {
            "symbol": symbol,
            "longName": clean_company_name(info.get("longName")),
            "shortName": clean_company_name(info.get("shortName")),
            "sector": info.get("sector"),
            "sectorKey": info.get("sectorKey"),
            "industry": info.get("industry"),
            "industryKey": info.get("industryKey"),
            "country": info.get("country"),
            "city": info.get("city"),
            "website": info.get("website"),
            "longBusinessSummary": info.get("longBusinessSummary"),
            "exchange": info.get("exchange"),
            "fullExchangeName": info.get("fullExchangeName"),
            "exchangeTimezoneName": info.get("exchangeTimezoneName"),
            "exchangeTimezoneShortName": info.get("exchangeTimezoneShortName"),
            "currency": info.get("currency"),
            "financialCurrency": info.get("financialCurrency"),
            "quoteType": info.get("quoteType"),
            "market": info.get("market"),
            "_range_start": _range_start,
            "_price_data_start": history_start
        }
    except: return None

def build_registries():
    for idx in INDICES:
        key = idx["key"]
        source = WIKI_SOURCES.get(key)
        if not source:
            print(f"\nSkipping {key}: no wiki source (provide dim.json manually)")
            continue

        output_file = data_path(key, "dim")
        output_file.parent.mkdir(parents=True, exist_ok=True)

        top_50_list = get_top_50_symbols(
            source["url"], source["match_col"],
            idx["name"], source.get("dot_to_dash", False)
        )

        final_records = []
        print(f"\nFinalizing Identity for {idx['name']}...")
        for sym in top_50_list:
            record = extract_full_identity(sym, idx["history_start"])
            if record:
                final_records.append(record)
            time.sleep(0.1)

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(final_records, f, indent=4, ensure_ascii=False)
        print(f"\nSuccess: Saved {len(final_records)} records to {output_file}")


if __name__ == "__main__":
    build_registries()