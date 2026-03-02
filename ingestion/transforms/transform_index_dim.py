"""SCD Type 2 merge: bronze.index_dim -> silver.index_dim.
Compares current bronze snapshot against silver.is_current rows.
- New symbols: INSERT with is_current=1
- Changed attributes: close old row (valid_to, is_current=0), insert new row
- Removed symbols: close old row
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import get_connection

COMPARE_COLS = [
    "long_name", "short_name", "sector", "sector_key",
    "industry", "industry_key", "country", "city", "website",
    "long_business_summary", "exchange", "full_exchange_name",
    "exchange_timezone_name", "exchange_timezone_short",
    "currency", "financial_currency", "quote_type", "market",
    "range_start", "price_data_start",
]

ALL_COLS = ["_index", "symbol"] + COMPARE_COLS


def run():
    conn = get_connection()
    cursor = conn.cursor()

    # Load bronze snapshot (latest per index+symbol)
    cursor.execute(f"""
        SELECT {', '.join(ALL_COLS)}
        FROM bronze.index_dim
    """)
    bronze_rows = cursor.fetchall()
    bronze_map = {}
    for row in bronze_rows:
        key = (row[0], row[1])  # (_index, symbol)
        bronze_map[key] = row

    # Load current silver rows
    cursor.execute(f"""
        SELECT {', '.join(ALL_COLS)}
        FROM silver.index_dim
        WHERE is_current = 1
    """)
    silver_rows = cursor.fetchall()
    silver_map = {}
    for row in silver_rows:
        key = (row[0], row[1])
        silver_map[key] = row

    inserted = 0
    updated = 0
    closed = 0

    # New or changed
    for key, b_row in bronze_map.items():
        if key not in silver_map:
            # New symbol -> insert
            _insert_row(cursor, b_row)
            inserted += 1
        else:
            s_row = silver_map[key]
            # Compare attribute columns (index 2 onward)
            if _has_changed(b_row, s_row):
                _close_row(cursor, key)
                _insert_row(cursor, b_row)
                updated += 1

    # Removed from index (in silver but not in bronze)
    for key in silver_map:
        if key not in bronze_map:
            _close_row(cursor, key)
            closed += 1

    conn.commit()
    cursor.close()
    conn.close()
    print(f"SCD2 index_dim: {inserted} new, {updated} changed, {closed} closed")


def _has_changed(b_row, s_row):
    """Compare attribute columns (skip _index, symbol at positions 0,1)."""
    for i in range(2, len(b_row)):
        bv = b_row[i]
        sv = s_row[i]
        # Normalize None vs empty string
        if bv is None and sv is None:
            continue
        if str(bv) != str(sv):
            return True
    return False


def _insert_row(cursor, row):
    placeholders = ", ".join(["?"] * len(ALL_COLS))
    cursor.execute(f"""
        INSERT INTO silver.index_dim ({', '.join(ALL_COLS)})
        VALUES ({placeholders})
    """, *row)


def _close_row(cursor, key):
    cursor.execute("""
        UPDATE silver.index_dim
        SET valid_to = SYSUTCDATETIME(), is_current = 0
        WHERE _index = ? AND symbol = ? AND is_current = 1
    """, key[0], key[1])


if __name__ == "__main__":
    run()
