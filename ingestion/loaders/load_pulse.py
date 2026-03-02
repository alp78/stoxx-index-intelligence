"""Loads pulse JSON into bronze.pulse. Idempotent: skips existing rows."""

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
             table="bronze.pulse")

    with open(json_file, 'r', encoding='utf-8') as f:
        records = json.load(f)

    conn = get_connection()
    cursor = conn.cursor()

    try:
        with StepTimer() as timer:
            cursor.execute(
                "SELECT symbol, timestamp FROM bronze.pulse WHERE _index = ?",
                index_name
            )
            existing = {(r[0], str(r[1])) for r in cursor.fetchall()}

            inserted = 0
            for rec in records:
                symbol = rec.get("symbol")
                if not symbol:
                    continue

                ts = rec.get("timestamp")
                if (symbol, ts) in existing:
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
                    index_name, symbol, ts,
                    p.get("current"), p.get("open"), p.get("dayHigh"), p.get("dayLow"),
                    p.get("previousClose"), p.get("change"), p.get("changePct"),
                    b.get("bid"), b.get("ask"), b.get("bidSize"), b.get("askSize"),
                    b.get("spread"),
                    v.get("current"), v.get("average10Day"), v.get("ratio")
                )
                inserted += 1

            conn.commit()

        log_info(logger, "Load complete", step="load", index=index_name,
                 records_inserted=inserted, records_skipped=len(records) - inserted,
                 records_total=len(records), duration_ms=timer.duration_ms)
    except Exception:
        log_error(logger, "Load failed", exc_info=True, step="load",
                  index=index_name, table="bronze.pulse")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    for key in get_all_keys():
        load(data_path(key, "pulse"), key)
