"""Loads price history JSON into per-index bronze OHLCV tables. Strategy: merge (insert missing only)."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import get_connection
from config import INDICES, data_path, bronze_ohlcv


def load(json_file, table):
    print(f"\n--- Loading OHLCV: {table} ---")

    with open(json_file, 'r', encoding='utf-8') as f:
        records = json.load(f)

    conn = get_connection()
    cursor = conn.cursor()

    # Get existing (symbol, date) pairs to skip duplicates
    cursor.execute(f"""
        SELECT symbol, CONVERT(VARCHAR(10), date, 120)
        FROM {table}
    """)
    existing = set((row[0], row[1]) for row in cursor.fetchall())

    inserted = 0
    skipped = 0

    for rec in records:
        symbol = rec.get("symbol")
        date = rec.get("date")
        if not symbol or not date:
            continue

        if (symbol, date) in existing:
            skipped += 1
            continue

        cursor.execute(f"""
            INSERT INTO {table} (
                symbol, date, [open], high, low, [close],
                adj_close, volume, dividends, stock_splits
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            symbol, date,
            rec.get("open"), rec.get("high"), rec.get("low"), rec.get("close"),
            rec.get("adj_close"), rec.get("volume"),
            rec.get("dividends"), rec.get("stock_splits")
        )
        inserted += 1

        if inserted % 10000 == 0:
            conn.commit()
            print(f"  {inserted:>8} inserted...", end="\r")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Done: {inserted} inserted, {skipped} skipped (already exist)")


if __name__ == "__main__":
    for idx in INDICES:
        key = idx["key"]
        load(data_path(key, "ohlcv_history"), bronze_ohlcv(key))
