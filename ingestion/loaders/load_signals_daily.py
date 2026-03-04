"""Loads daily signals JSON into bronze.signals_daily. Strategy: truncate & reload per index."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from utils.db import get_connection
from utils.config import data_path, get_all_keys
from utils.logger import get_logger, log_info, log_error, StepTimer

logger = get_logger(__name__)


def load(json_file, index_name):
    log_info(logger, "Loading daily signals into bronze (truncate & reload for this index)",
             step="load", index=index_name, table="bronze.signals_daily")

    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            records = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        log_error(logger, "Cannot load daily signals — JSON file missing or corrupt",
                  step="load", index=index_name, file=str(json_file), error=str(e))
        return

    conn = get_connection()
    cursor = conn.cursor()

    try:
        with StepTimer() as timer:
            cursor.execute("DELETE FROM bronze.signals_daily WHERE _index = ?", index_name)

            rows = []
            for rec in records:
                symbol = rec.get("symbol")
                if not symbol:
                    continue

                ts = rec.get("timestamp")
                pm = rec.get("price_metrics", {})
                mc = rec.get("market_context", {})
                ms = rec.get("momentum_signals", {})
                ss = rec.get("sentiment_signals", {})

                rows.append((
                    index_name, symbol, ts,
                    pm.get("currentPrice"), pm.get("forwardPE"),
                    pm.get("priceToBook"), pm.get("evToEbitda"),
                    pm.get("dividendYield"), pm.get("marketCap"), pm.get("beta"),
                    mc.get("fiftyTwoWeekChange"), mc.get("SandP52WeekChange"),
                    ms.get("fiftyDayAverage"), ms.get("twoHundredDayAverage"),
                    ms.get("distFrom52WeekHigh"),
                    ss.get("targetMedianPrice"), ss.get("recommendationMean"),
                    ss.get("upsidePotential")
                ))

            cursor.fast_executemany = True
            cursor.executemany("""
                INSERT INTO bronze.signals_daily (
                    _index, symbol, timestamp,
                    current_price, forward_pe, price_to_book, ev_to_ebitda,
                    dividend_yield, market_cap, beta,
                    fifty_two_week_change, sandp_52_week_change,
                    fifty_day_average, two_hundred_day_average, dist_from_52_week_high,
                    target_median_price, recommendation_mean, upside_potential
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)
            inserted = len(rows)
            conn.commit()

        log_info(logger, "Daily signals load complete — bronze refreshed with latest snapshot",
                 step="load", index=index_name, records_inserted=inserted,
                 duration_ms=timer.duration_ms)
    except Exception:
        conn.rollback()
        log_error(logger, "Daily signals load failed — rolling back", exc_info=True,
                  step="load", index=index_name, table="bronze.signals_daily")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    for key in get_all_keys():
        load(data_path(key, "signals_daily"), key)
