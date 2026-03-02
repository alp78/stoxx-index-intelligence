"""Upsert transform: bronze.signals_quarterly -> silver.signals_quarterly.

Bronze holds only the current snapshot (truncate & reload per run).
Groups by actual fiscal quarter (most_recent_quarter from yfinance).
This transform compares bronze against silver and:
  - Inserts a new row when a company reports a new quarter
  - Updates the existing silver row if fundamentals have been revised
  - Skips if values are identical
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from utils.db import get_connection
from utils.logger import get_logger, log_info, log_error, StepTimer

logger = get_logger(__name__)

_VALUE_COLS = [
    "gross_margins", "operating_margins", "return_on_equity",
    "revenue_growth", "earnings_growth",
    "shares_outstanding", "float_shares", "debt_to_equity",
    "current_ratio", "free_cashflow",
    "last_fiscal_year_end", "most_recent_quarter",
    "overall_risk", "audit_risk", "board_risk",
    "compensation_risk", "shareholder_rights_risk", "esg_populated",
]

_ALL_COLS = ["_index", "symbol", "as_of_date"] + _VALUE_COLS


def run():
    log_info(logger, "Upserting quarterly signals from bronze to silver (keyed by fiscal quarter)",
             step="transform", target="silver.signals_quarterly")

    conn = get_connection()
    cursor = conn.cursor()

    try:
        with StepTimer() as timer:
            # Load existing silver data for comparison
            cursor.execute(f"""
                SELECT _index, symbol, CONVERT(VARCHAR(10), as_of_date, 120),
                       {', '.join(_VALUE_COLS)}
                FROM silver.signals_quarterly
            """)
            silver = {}
            for r in cursor.fetchall():
                silver[(r[0], r[1], r[2])] = tuple(r[3:])

            # Read current bronze snapshot.
            # Use most_recent_quarter as silver as_of_date (actual fiscal quarter).
            # Fall back to as_of_date if most_recent_quarter is NULL.
            cursor.execute(f"""
                SELECT _index, symbol,
                       COALESCE(most_recent_quarter, as_of_date) AS as_of_date,
                       {', '.join(_VALUE_COLS)}
                FROM bronze.signals_quarterly
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
                        INSERT INTO silver.signals_quarterly ({', '.join(_ALL_COLS)})
                        VALUES ({placeholders})
                    """, *list(row))
                    inserted += 1
                elif silver[key] != values:
                    cursor.execute(f"""
                        UPDATE silver.signals_quarterly
                        SET {set_clause}
                        WHERE _index = ? AND symbol = ? AND as_of_date = ?
                    """, *list(row[3:]), row[0], row[1], row[2])
                    updated += 1
                else:
                    unchanged += 1

            conn.commit()

        log_info(logger, "Quarterly signals upsert complete — silver updated with latest fundamentals",
                 step="transform", target="silver.signals_quarterly",
                 records_inserted=inserted, records_updated=updated,
                 records_unchanged=unchanged, duration_ms=timer.duration_ms)
    except Exception:
        conn.rollback()
        log_error(logger, "Quarterly signals upsert failed", exc_info=True,
                  step="transform", target="silver.signals_quarterly")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    run()
