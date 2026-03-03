"""Fetches quarterly fundamental signals (margins, leverage, governance) from yfinance."""

import sys
import yfinance as yf
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from utils.config import INDICES, data_path, safe_write_json, format_epoch, cet_now_str
from utils.db import get_index_symbols
from utils.logger import get_logger, log_info, log_error, StepTimer

logger = get_logger(__name__)


def fetch_quarterly_fundamentals(index_key, output_file):
    log_info(logger, "Fetching quarterly fundamentals (margins, leverage, governance) from yfinance",
             step="fetch", source="yfinance", kind="signals_quarterly", index=index_key)

    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    all_fundamentals = []

    # Read symbol list from bronze.index_dim (populated by setup_index.py)
    symbols = [r[0] for r in get_index_symbols(index_key)]
    if not symbols:
        log_error(logger, "No symbols in bronze.index_dim — run setup_index.py first",
                  step="fetch", index=index_key)
        return
    registry = [{"symbol": s} for s in symbols]

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
                    "as_of_date": cet_now_str('%Y-%m-%d'),
                    "quality_metrics": {
                        "grossMargins": info.get("grossMargins"),
                        "operatingMargins": info.get("operatingMargins"),
                        "returnOnEquity": info.get("returnOnEquity"),
                        "revenueGrowth": info.get("revenueGrowth"),
                        "earningsGrowth": info.get("earningsGrowth")
                    },
                    "capital_structure": {
                        "sharesOutstanding": info.get("sharesOutstanding"),
                        "floatShares": info.get("floatShares") if info.get("floatShares") is not None else info.get("sharesOutstanding"),
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
                        "esgPopulated": info.get("overallRisk") is not None
                    }
                }
                all_fundamentals.append(record)
            except Exception as e:
                log_error(logger, "Failed to fetch quarterly fundamentals for symbol",
                          step="fetch", symbol=symbol, error=str(e))

            time.sleep(0.15)

    safe_write_json(output_file, all_fundamentals)

    log_info(logger, "Quarterly fundamentals fetch complete — wrote JSON snapshot",
             step="fetch", kind="signals_quarterly",
             records_fetched=len(all_fundamentals), duration_ms=timer.duration_ms)


if __name__ == "__main__":
    for idx in INDICES:
        key = idx["key"]
        fetch_quarterly_fundamentals(key, data_path(key, "signals_quarterly"))
