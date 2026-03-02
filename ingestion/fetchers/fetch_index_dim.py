"""Fetches index dimension data from definition files + yfinance.

Reads the stock list from ingestion/definitions/{key}.json,
enriches each symbol via yfinance, and outputs data/stage/{prefix}_dim.json.
"""

import json
import sys
import time
import re
from pathlib import Path

import yfinance as yf

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from utils.config import INDICES, get_index, data_path, definition_path, safe_write_json, format_epoch
from utils.logger import get_logger, log_info, log_warning, log_error, StepTimer

logger = get_logger(__name__)


def clean_company_name(raw_name):
    if not raw_name:
        return None
    cleaned = re.sub(r'\s{2,}.*', '', str(raw_name))
    return cleaned.strip()


def extract_full_identity(symbol, history_start="2021-01-01"):
    """Extracts identity fields from yfinance into a flat dictionary."""
    try:
        tkr = yf.Ticker(symbol)
        info = tkr.info

        _range_start = format_epoch(info.get("firstTradeDateMilliseconds"), is_ms=True)

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
    except Exception as e:
        log_error(logger, "Failed to extract identity fields from yfinance for symbol",
                  step="fetch", symbol=symbol, error=str(e))
        return None


def build_registry(key):
    """Build dim registry for a single index."""
    idx = get_index(key)
    def_file = definition_path(key)

    if not def_file.exists():
        log_warning(logger, "Cannot build registry — no definition file found",
                    step="fetch", index=key, expected=str(def_file))
        return

    with open(def_file, "r", encoding="utf-8") as f:
        defn = json.load(f)

    symbols = defn.get("symbols", [])
    if not symbols:
        log_warning(logger, "Cannot build registry — definition file has no symbols",
                    step="fetch", index=key)
        return

    output_file = data_path(key, "dim")
    output_file.parent.mkdir(parents=True, exist_ok=True)

    log_info(logger, "Enriching stock list via yfinance to build dimension registry",
             step="fetch", index=key, symbols_count=len(symbols))

    with StepTimer() as timer:
        final_records = []
        for sym in symbols:
            record = extract_full_identity(sym, idx["history_start"])
            if record:
                final_records.append(record)
            else:
                log_warning(logger, "Symbol skipped — yfinance enrichment failed or stock too recent",
                            step="fetch", index=key, symbol=sym)
            time.sleep(0.1)

    safe_write_json(output_file, final_records)

    log_info(logger, "Dimension registry built — enriched stock identities written to JSON",
             step="fetch", index=key, symbols_input=len(symbols),
             records_fetched=len(final_records), duration_ms=timer.duration_ms)


def build_registries():
    """Build dim registries for all indices."""
    for idx in INDICES:
        build_registry(idx["key"])


if __name__ == "__main__":
    build_registries()
