"""Dedup transform: bronze.signals_quarterly -> silver.signals_quarterly.
Takes the latest row per (index, symbol, as_of_date), skips rows already in silver."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import get_connection

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
    conn = get_connection()
    cursor = conn.cursor()

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
    cursor.close()
    conn.close()
    print(f"Quarterly signals: {inserted} inserted, {skipped} skipped")


if __name__ == "__main__":
    run()
