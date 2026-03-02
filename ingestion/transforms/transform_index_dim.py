"""SCD Type 2 merge: bronze.index_dim -> silver.index_dim.
Compares current bronze snapshot against silver.is_current rows.
- New symbols: INSERT with is_current=1
- Changed attributes: close old row (valid_to, is_current=0), insert new row
- Removed symbols: close old row
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from utils.db import get_connection
from utils.logger import get_logger, log_info, log_error, StepTimer

logger = get_logger(__name__)

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
    log_info(logger, "Running SCD Type 2 merge — tracking new, changed, and removed symbols in silver",
             step="transform", target="silver.index_dim", type="SCD2")

    conn = get_connection()
    cursor = conn.cursor()

    try:
        with StepTimer() as timer:
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
                    _insert_row(cursor, b_row)
                    inserted += 1
                else:
                    s_row = silver_map[key]
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

        log_info(logger, "Index dimension SCD2 merge complete — new/changed/removed symbols tracked in silver",
                 step="transform", target="silver.index_dim",
                 records_inserted=inserted, records_updated=updated,
                 records_closed=closed, duration_ms=timer.duration_ms)
    except Exception:
        conn.rollback()
        log_error(logger, "Index dimension SCD2 merge failed", exc_info=True,
                  step="transform", target="silver.index_dim")
        raise
    finally:
        cursor.close()
        conn.close()


def _has_changed(b_row, s_row):
    """Compare attribute columns (skip _index, symbol at positions 0,1).

    Normalizes types before comparing to avoid false positives from
    driver type differences (e.g. datetime.date vs str, Decimal vs float).
    """
    for i in range(2, len(b_row)):
        bv = b_row[i]
        sv = s_row[i]
        if bv is None and sv is None:
            continue
        if bv is None or sv is None:
            return True
        # Normalize: compare as strings with consistent formatting
        # Strip trailing zeros from numeric-like values to avoid 1.0 != 1 issues
        bs = str(bv).rstrip('0').rstrip('.') if isinstance(bv, (int, float)) else str(bv)
        ss = str(sv).rstrip('0').rstrip('.') if isinstance(sv, (int, float)) else str(sv)
        if bs != ss:
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
