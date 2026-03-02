"""Loads daily OHLCV JSON into per-index bronze OHLCV tables. Strategy: merge (insert missing only)."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import get_connection

TABLES = {
    "euro_stoxx": "bronze.eurostoxx50_ohlcv",
    "stoxx_usa": "bronze.stoxxusa50_ohlcv"
}


def load(json_file, index_name):
    table = TABLES[index_name]
    print(f"\n--- Loading daily OHLCV: {table} ---")

    with open(json_file, 'r', encoding='utf-8') as f:
        records = json.load(f)

    conn = get_connection()
    cursor = conn.cursor()

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

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Done: {inserted} inserted, {skipped} skipped (already exist)")


if __name__ == "__main__":
    history_dir = Path(__file__).resolve().parent.parent.parent / "data" / "history"

    load(history_dir / "eurostoxx50_ohlcv_daily.json", "euro_stoxx")
    load(history_dir / "stoxxusa50_ohlcv_daily.json", "stoxx_usa")
