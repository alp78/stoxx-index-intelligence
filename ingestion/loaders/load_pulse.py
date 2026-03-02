"""Loads pulse JSON into bronze.pulse. Idempotent: skips existing rows."""

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
    log_info(logger, "Loading pulse snapshots into bronze (merge — skip existing timestamps)",
             step="load", index=index_name, table="bronze.pulse")

    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            records = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        log_error(logger, "Cannot load pulse — JSON file missing or corrupt",
                  step="load", index=index_name, file=str(json_file), error=str(e))
        return

    conn = get_connection()
    cursor = conn.cursor()

    try:
        with StepTimer() as timer:
            cursor.execute(
                "SELECT symbol, CONVERT(VARCHAR(19), timestamp, 120) FROM bronze.pulse WHERE _index = ?",
                index_name
            )
            existing = {(r[0], r[1]) for r in cursor.fetchall()}

            inserted = 0
            skipped = 0
            for rec in records:
                symbol = rec.get("symbol")
                if not symbol:
                    continue

                ts = rec.get("timestamp")
                if (symbol, ts) in existing:
                    skipped += 1
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

        log_info(logger, "Pulse load complete — new snapshots merged into bronze",
                 step="load", index=index_name, records_inserted=inserted,
                 records_skipped=skipped, records_total=len(records),
                 duration_ms=timer.duration_ms)
    except Exception:
        conn.rollback()
        log_error(logger, "Pulse load failed — rolling back", exc_info=True,
                  step="load", index=index_name, table="bronze.pulse")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    for key in get_all_keys():
        load(data_path(key, "pulse"), key)
