"""Loads daily signals JSON into bronze.signals_daily. Idempotent: skips existing rows."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import get_connection
from config import INDICES, data_path, get_all_keys
from logger import get_logger, log_info, log_error, StepTimer

logger = get_logger(__name__)


def load(json_file, index_name):
    log_info(logger, "Load started", step="load", index=index_name,
             table="bronze.signals_daily")

    with open(json_file, 'r', encoding='utf-8') as f:
        records = json.load(f)

    conn = get_connection()
    cursor = conn.cursor()

    try:
        with StepTimer() as timer:
            cursor.execute(
                "SELECT symbol, timestamp FROM bronze.signals_daily WHERE _index = ?",
                index_name
            )
            existing = {(r[0], str(r[1])) for r in cursor.fetchall()}

            inserted = 0
            for rec in records:
                symbol = rec.get("symbol")
                if not symbol:
                    continue

                ts = rec.get("timestamp")
                if (symbol, ts) in existing:
                    continue

                pm = rec.get("price_metrics", {})
                mc = rec.get("market_context", {})
                ms = rec.get("momentum_signals", {})
                ss = rec.get("sentiment_signals", {})

                cursor.execute("""
                    INSERT INTO bronze.signals_daily (
                        _index, symbol, timestamp,
                        current_price, forward_pe, price_to_book, ev_to_ebitda,
                        dividend_yield, market_cap, beta,
                        fifty_two_week_change, sandp_52_week_change,
                        fifty_day_average, two_hundred_day_average, dist_from_52_week_high,
                        target_median_price, recommendation_mean, upside_potential
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    index_name, symbol, ts,
                    pm.get("currentPrice"), pm.get("forwardPE"),
                    pm.get("priceToBook"), pm.get("evToEbitda"),
                    pm.get("dividendYield"), pm.get("marketCap"), pm.get("beta"),
                    mc.get("fiftyTwoWeekChange"), mc.get("SandP52WeekChange"),
                    ms.get("fiftyDayAverage"), ms.get("twoHundredDayAverage"),
                    ms.get("distFrom52WeekHigh"),
                    ss.get("targetMedianPrice"), ss.get("recommendationMean"),
                    ss.get("upsidePotential")
                )
                inserted += 1

            conn.commit()

        log_info(logger, "Load complete", step="load", index=index_name,
                 records_inserted=inserted, records_skipped=len(records) - inserted,
                 records_total=len(records), duration_ms=timer.duration_ms)
    except Exception:
        log_error(logger, "Load failed", exc_info=True, step="load",
                  index=index_name, table="bronze.signals_daily")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    for key in get_all_keys():
        load(data_path(key, "signals_daily"), key)
