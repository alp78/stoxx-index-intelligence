"""Loads pulse tickers JSON into bronze.pulse_tickers. Strategy: truncate & reload per index."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from utils.db import get_connection
from utils.config import data_path, get_all_keys
from utils.logger import get_logger, log_info, log_error, StepTimer

logger = get_logger(__name__)


def load(json_file, index_name):
    log_info(logger, "Loading pulse tickers into bronze (truncate & reload for this index)",
             step="load", index=index_name, table="bronze.pulse_tickers")

    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        log_error(logger, "Cannot load pulse tickers — JSON file missing or corrupt",
                  step="load", index=index_name, file=str(json_file), error=str(e))
        return

    conn = get_connection()
    cursor = conn.cursor()

    try:
        with StepTimer() as timer:
            cursor.execute("DELETE FROM bronze.pulse_tickers WHERE _index = ?", index_name)

            discovered_at = data.get("discovered_at")
            ranking = data.get("ranking", [])

            inserted = 0
            for i, item in enumerate(ranking):
                symbol = item.get("symbol")
                if not symbol:
                    continue

                cursor.execute("""
                    INSERT INTO bronze.pulse_tickers (
                        _index, discovered_at, symbol, rank,
                        volume_surge, range_intensity, vol_z, rng_z, activity_score
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    index_name, discovered_at, symbol, i + 1,
                    item.get("volumeSurge"), item.get("rangeIntensity"),
                    item.get("volZ"), item.get("rngZ"), item.get("activityScore")
                )
                inserted += 1

            conn.commit()

        log_info(logger, "Pulse tickers load complete — bronze refreshed with latest ranking",
                 step="load", index=index_name, records_inserted=inserted,
                 duration_ms=timer.duration_ms)
    except Exception:
        conn.rollback()
        log_error(logger, "Pulse tickers load failed — rolling back", exc_info=True,
                  step="load", index=index_name, table="bronze.pulse_tickers")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    for key in get_all_keys():
        load(data_path(key, "tickers"), key)
