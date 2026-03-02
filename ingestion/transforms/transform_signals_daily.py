"""Upsert transform: bronze.signals_daily -> silver.signals_daily.

Bronze holds only the current day's snapshot (truncate & reload per run).
This transform compares bronze against silver and:
  - Inserts a new row if the date doesn't exist in silver yet
  - Updates the existing silver row if values have changed
  - Skips if values are identical (market hasn't moved since last fetch)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from utils.db import get_connection
from utils.logger import get_logger, log_info, log_error, StepTimer

logger = get_logger(__name__)

_VALUE_COLS = [
    "current_price", "forward_pe", "price_to_book", "ev_to_ebitda",
    "dividend_yield", "market_cap", "beta",
    "fifty_two_week_change", "sandp_52_week_change",
    "fifty_day_average", "two_hundred_day_average", "dist_from_52_week_high",
    "target_median_price", "recommendation_mean", "upside_potential",
]

_ALL_COLS = ["_index", "symbol", "signal_date"] + _VALUE_COLS


def run():
    log_info(logger, "Upserting daily signals from bronze to silver (insert new dates, update changed values)",
             step="transform", target="silver.signals_daily")

    conn = get_connection()
    cursor = conn.cursor()

    try:
        with StepTimer() as timer:
            # Load existing silver data for comparison
            cursor.execute(f"""
                SELECT _index, symbol, CONVERT(VARCHAR(10), signal_date, 120),
                       {', '.join(_VALUE_COLS)}
                FROM silver.signals_daily
            """)
            silver = {}
            for r in cursor.fetchall():
                silver[(r[0], r[1], r[2])] = tuple(r[3:])

            # Read current bronze snapshot (one row per symbol per index)
            cursor.execute(f"""
                SELECT _index, symbol, CAST(timestamp AS DATE) AS signal_date,
                       {', '.join(_VALUE_COLS)}
                FROM bronze.signals_daily
            """)

            inserted = 0
            updated = 0
            unchanged = 0

            set_clause = ", ".join(f"{c} = ?" for c in _VALUE_COLS)
            placeholders = ", ".join(["?"] * len(_ALL_COLS))

            for row in cursor.fetchall():
                key = (row[0], row[1], str(row[2]))
                values = tuple(row[3:])

                if key not in silver:
                    cursor.execute(f"""
                        INSERT INTO silver.signals_daily ({', '.join(_ALL_COLS)})
                        VALUES ({placeholders})
                    """, *list(row))
                    inserted += 1
                elif silver[key] != values:
                    cursor.execute(f"""
                        UPDATE silver.signals_daily
                        SET {set_clause}
                        WHERE _index = ? AND symbol = ? AND signal_date = ?
                    """, *list(row[3:]), row[0], row[1], row[2])
                    updated += 1
                else:
                    unchanged += 1

            conn.commit()

        log_info(logger, "Daily signals upsert complete — silver updated with latest market data",
                 step="transform", target="silver.signals_daily",
                 records_inserted=inserted, records_updated=updated,
                 records_unchanged=unchanged, duration_ms=timer.duration_ms)
    except Exception:
        conn.rollback()
        log_error(logger, "Daily signals upsert failed", exc_info=True,
                  step="transform", target="silver.signals_daily")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    run()
