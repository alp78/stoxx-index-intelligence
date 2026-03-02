"""Fetches daily trading signals (PE, dividend yield, moving averages, etc.) from yfinance."""

import json
import sys
import yfinance as yf
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import INDICES, data_path
from logger import get_logger, log_info, log_warning, log_error, StepTimer

logger = get_logger(__name__)


def fetch_daily_signals(reg_file, output_file):
    log_info(logger, "Fetch started", step="fetch", source="yfinance",
             kind="signals_daily", reg_file=str(reg_file))

    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    daily_signals = []

    try:
        with open(reg_file, 'r', encoding='utf-8') as f:
            registry = json.load(f)
    except FileNotFoundError:
        log_error(logger, "Registry file not found", step="fetch",
                  reg_file=str(reg_file))
        return

    with StepTimer() as timer:
        for company in registry:
            symbol = company.get('symbol')

            if not symbol:
                continue

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
                log_error(logger, "Symbol fetch failed", step="fetch",
                          symbol=symbol, error=str(e))

            time.sleep(0.15)

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(daily_signals, f, indent=4, ensure_ascii=False)

    log_info(logger, "Fetch complete", step="fetch", kind="signals_daily",
             records_fetched=len(daily_signals), duration_ms=timer.duration_ms)


if __name__ == "__main__":
    for idx in INDICES:
        key = idx["key"]
        fetch_daily_signals(
            reg_file=data_path(key, "dim"),
            output_file=data_path(key, "signals_daily")
        )
