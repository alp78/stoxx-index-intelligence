"""Fetches quarterly fundamental signals (margins, leverage, governance) from yfinance."""

import json
import sys
import yfinance as yf
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import INDICES, data_path
from logger import get_logger, log_info, log_error, StepTimer

logger = get_logger(__name__)


def format_epoch(epoch_val):
    """Safely converts Unix timestamps to YYYY-MM-DD, bypassing Windows OS limits."""
    if epoch_val is None:
        return None
    try:
        epoch_dt = datetime(1970, 1, 1)
        return (epoch_dt + timedelta(seconds=float(epoch_val))).strftime('%Y-%m-%d')
    except Exception:
        return None


def fetch_quarterly_fundamentals(reg_file, output_file):
    log_info(logger, "Fetch started", step="fetch", source="yfinance",
             kind="signals_quarterly", reg_file=str(reg_file))

    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    all_fundamentals = []

    try:
        with open(reg_file, 'r', encoding='utf-8') as f:
            registry = json.load(f)
    except FileNotFoundError:
        log_error(logger, "Registry file not found", step="fetch",
                  reg_file=str(reg_file))
        return

    with StepTimer() as timer:
        for company in registry:
            symbol = company.get('symbol') or company.get('static', {}).get('symbol')

            if not symbol:
                continue

            try:
                tkr = yf.Ticker(symbol)
                info = tkr.info

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
                log_error(logger, "Symbol fetch failed", step="fetch",
                          symbol=symbol, error=str(e))

            time.sleep(0.15)

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_fundamentals, f, indent=4, ensure_ascii=False)

    log_info(logger, "Fetch complete", step="fetch", kind="signals_quarterly",
             records_fetched=len(all_fundamentals), duration_ms=timer.duration_ms)


if __name__ == "__main__":
    for idx in INDICES:
        key = idx["key"]
        fetch_quarterly_fundamentals(
            reg_file=data_path(key, "dim"),
            output_file=data_path(key, "signals_quarterly")
        )
