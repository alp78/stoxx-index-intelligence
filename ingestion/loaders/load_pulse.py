"""Loads pulse JSON into bronze.pulse. Strategy: append."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import get_connection


def load(json_file, index_name):
    print(f"\n--- Loading pulse: {index_name} ---")

    with open(json_file, 'r', encoding='utf-8') as f:
        records = json.load(f)

    conn = get_connection()
    cursor = conn.cursor()

    for rec in records:
        symbol = rec.get("symbol")
        if not symbol:
            continue

        p = rec.get("price", {})
        b = rec.get("book", {})
        v = rec.get("volume", {})

        cursor.execute("""
            INSERT INTO bronze.pulse (
                _index, symbol, timestamp,
                current_price, open_price, day_high, day_low,
                previous_close, price_change, price_change_pct,
                bid, ask, bid_size, ask_size, spread,
                current_volume, average_volume_10day, volume_ratio
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            index_name, symbol, rec.get("timestamp"),
            p.get("current"), p.get("open"), p.get("dayHigh"), p.get("dayLow"),
            p.get("previousClose"), p.get("change"), p.get("changePct"),
            b.get("bid"), b.get("ask"), b.get("bidSize"), b.get("askSize"),
            b.get("spread"),
            v.get("current"), v.get("average10Day"), v.get("ratio")
        )

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Done: {len(records)} records appended for {index_name}")


if __name__ == "__main__":
    pulse_dir = Path(__file__).resolve().parent.parent.parent / "data" / "pulse"

    load(pulse_dir / "euro_stoxx_10_pulse.json", "euro_stoxx")
    load(pulse_dir / "stoxx_usa_10_pulse.json", "stoxx_usa")
