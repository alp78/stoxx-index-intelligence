"""Loads pulse tickers JSON into bronze.pulse_tickers. Strategy: truncate & reload per index."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import get_connection


def load(json_file, index_name):
    print(f"\n--- Loading pulse tickers: {index_name} ---")

    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM bronze.pulse_tickers WHERE _index = ?", index_name)

    discovered_at = data.get("discovered_at")
    ranking = data.get("ranking", [])

    for i, item in enumerate(ranking):
        cursor.execute("""
            INSERT INTO bronze.pulse_tickers (
                _index, discovered_at, symbol, rank,
                volume_surge, range_intensity, vol_z, rng_z, activity_score
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            index_name, discovered_at, item.get("symbol"), i + 1,
            item.get("volumeSurge"), item.get("rangeIntensity"),
            item.get("volZ"), item.get("rngZ"), item.get("activityScore")
        )

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Done: {len(ranking)} tickers loaded for {index_name}")


if __name__ == "__main__":
    pulse_dir = Path(__file__).resolve().parent.parent.parent / "data" / "pulse"

    load(pulse_dir / "eurostoxx50_tickers.json", "euro_stoxx")
    load(pulse_dir / "stoxxusa50_tickers.json", "stoxx_usa")
