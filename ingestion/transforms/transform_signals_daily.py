"""Dedup transform: bronze.signals_daily -> silver.signals_daily.
Takes the latest row per (index, symbol, date), skips rows already in silver."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import get_connection
from logger import get_logger, log_info, log_error, StepTimer

logger = get_logger(__name__)

COLS = [
    "_index", "symbol", "signal_date",
    "current_price", "forward_pe", "price_to_book", "ev_to_ebitda",
    "dividend_yield", "market_cap", "beta",
    "fifty_two_week_change", "sandp_52_week_change",
    "fifty_day_average", "two_hundred_day_average", "dist_from_52_week_high",
    "target_median_price", "recommendation_mean", "upside_potential",
]


def run():
    log_info(logger, "Transform started", step="transform",
             target="silver.signals_daily")

    conn = get_connection()
    cursor = conn.cursor()

    try:
        with StepTimer() as timer:
            # Get existing silver keys to skip
            cursor.execute("""
                SELECT _index, symbol, CONVERT(VARCHAR(10), signal_date, 120)
                FROM silver.signals_daily
            """)
            existing = set((r[0], r[1], r[2]) for r in cursor.fetchall())

            # Deduplicated bronze: latest row per (index, symbol, date)
            cursor.execute("""
                WITH ranked AS (
                    SELECT _index, symbol,
                           CAST(timestamp AS DATE) AS signal_date,
                           current_price, forward_pe, price_to_book, ev_to_ebitda,
                           dividend_yield, market_cap, beta,
                           fifty_two_week_change, sandp_52_week_change,
                           fifty_day_average, two_hundred_day_average, dist_from_52_week_high,
                           target_median_price, recommendation_mean, upside_potential,
                           ROW_NUMBER() OVER (
                               PARTITION BY _index, symbol, CAST(timestamp AS DATE)
                               ORDER BY timestamp DESC
                           ) AS rn
                    FROM bronze.signals_daily
                )
                SELECT _index, symbol, signal_date,
                       current_price, forward_pe, price_to_book, ev_to_ebitda,
                       dividend_yield, market_cap, beta,
                       fifty_two_week_change, sandp_52_week_change,
                       fifty_day_average, two_hundred_day_average, dist_from_52_week_high,
                       target_median_price, recommendation_mean, upside_potential
                FROM ranked WHERE rn = 1
            """)

            inserted = 0
            skipped = 0

            for row in cursor.fetchall():
                key = (row[0], row[1], str(row[2]))
                if key in existing:
                    skipped += 1
                    continue

                placeholders = ", ".join(["?"] * len(COLS))
                cursor.execute(f"""
                    INSERT INTO silver.signals_daily ({', '.join(COLS)})
                    VALUES ({placeholders})
                """, *row)
                inserted += 1

            conn.commit()

        log_info(logger, "Transform complete", step="transform",
                 target="silver.signals_daily", records_inserted=inserted,
                 records_skipped=skipped, duration_ms=timer.duration_ms)
    except Exception:
        log_error(logger, "Transform failed", exc_info=True,
                  step="transform", target="silver.signals_daily")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    run()
