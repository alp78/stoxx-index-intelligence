"""Dedup transform: bronze.signals_quarterly -> silver.signals_quarterly.
Takes the latest row per (index, symbol, as_of_date), skips rows already in silver."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import get_connection
from logger import get_logger, log_info, log_error, StepTimer

logger = get_logger(__name__)

COLS = [
    "_index", "symbol", "as_of_date",
    "gross_margins", "operating_margins", "return_on_equity",
    "revenue_growth", "earnings_growth",
    "shares_outstanding", "float_shares", "debt_to_equity",
    "current_ratio", "free_cashflow",
    "last_fiscal_year_end", "most_recent_quarter",
    "overall_risk", "audit_risk", "board_risk",
    "compensation_risk", "shareholder_rights_risk", "esg_populated",
]


def run():
    log_info(logger, "Transform started", step="transform",
             target="silver.signals_quarterly")

    conn = get_connection()
    cursor = conn.cursor()

    try:
        with StepTimer() as timer:
            # Get existing silver keys
            cursor.execute("""
                SELECT _index, symbol, CONVERT(VARCHAR(10), as_of_date, 120)
                FROM silver.signals_quarterly
            """)
            existing = set((r[0], r[1], r[2]) for r in cursor.fetchall())

            # Deduplicated bronze: latest ingestion per (index, symbol, as_of_date)
            cursor.execute("""
                WITH ranked AS (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY _index, symbol, as_of_date
                               ORDER BY _ingested_at DESC
                           ) AS rn
                    FROM bronze.signals_quarterly
                )
                SELECT _index, symbol, as_of_date,
                       gross_margins, operating_margins, return_on_equity,
                       revenue_growth, earnings_growth,
                       shares_outstanding, float_shares, debt_to_equity,
                       current_ratio, free_cashflow,
                       last_fiscal_year_end, most_recent_quarter,
                       overall_risk, audit_risk, board_risk,
                       compensation_risk, shareholder_rights_risk, esg_populated
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
                    INSERT INTO silver.signals_quarterly ({', '.join(COLS)})
                    VALUES ({placeholders})
                """, *row)
                inserted += 1

            conn.commit()

        log_info(logger, "Transform complete", step="transform",
                 target="silver.signals_quarterly", records_inserted=inserted,
                 records_skipped=skipped, duration_ms=timer.duration_ms)
    except Exception:
        log_error(logger, "Transform failed", exc_info=True,
                  step="transform", target="silver.signals_quarterly")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    run()
