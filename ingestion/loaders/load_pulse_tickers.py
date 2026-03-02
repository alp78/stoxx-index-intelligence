"""Loads pulse tickers JSON into bronze.pulse_tickers. Strategy: truncate & reload per index."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import get_connection
from config import INDICES, data_path, get_all_keys
from logger import get_logger, log_info, log_error, StepTimer

logger = get_logger(__name__)


def load(json_file, index_name):
    log_info(logger, "Load started", step="load", index=index_name,
             table="bronze.pulse_tickers")

    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    conn = get_connection()
    cursor = conn.cursor()

    try:
        with StepTimer() as timer:
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

        log_info(logger, "Load complete", step="load", index=index_name,
                 records_inserted=len(ranking), duration_ms=timer.duration_ms)
    except Exception:
        conn.rollback()
        log_error(logger, "Load failed", exc_info=True, step="load",
                  index=index_name, table="bronze.pulse_tickers")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    for key in get_all_keys():
        load(data_path(key, "tickers"), key)
