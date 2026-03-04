"""Loads OHLCV JSON into per-index bronze OHLCV tables. Strategy: merge (insert missing only)."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from utils.db import get_connection
from utils.config import INDICES, data_path, bronze_ohlcv
from utils.logger import get_logger, log_info, log_error, StepTimer

logger = get_logger(__name__)


def load(json_file, table):
    log_info(logger, "Loading OHLCV prices from JSON into bronze (merge — insert missing rows only)",
             step="load", table=table)

    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            records = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        log_error(logger, "Cannot load OHLCV — JSON file missing or corrupt",
                  step="load", table=table, file=str(json_file), error=str(e))
        return

    conn = get_connection()
    cursor = conn.cursor()

    try:
        with StepTimer() as timer:
            cursor.execute(f"""
                SELECT symbol, CONVERT(VARCHAR(10), date, 120)
                FROM {table}
            """)
            existing = set((row[0], row[1]) for row in cursor.fetchall())

            rows = []
            skipped = 0

            for rec in records:
                symbol = rec.get("symbol")
                date = rec.get("date")
                if not symbol or not date:
                    continue

                if (symbol, date) in existing:
                    skipped += 1
                    continue

                rows.append((
                    symbol, date,
                    rec.get("open"), rec.get("high"), rec.get("low"), rec.get("close"),
                    rec.get("adj_close"), rec.get("volume"),
                    rec.get("dividends"), rec.get("stock_splits")
                ))

            cursor.fast_executemany = True
            cursor.executemany(f"""
                INSERT INTO {table} (
                    symbol, date, [open], high, low, [close],
                    adj_close, volume, dividends, stock_splits
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)
            inserted = len(rows)
            conn.commit()

        log_info(logger, "OHLCV load complete — merged new rows into bronze, skipped existing",
                 step="load", table=table, records_inserted=inserted,
                 records_skipped=skipped, records_total=len(records),
                 duration_ms=timer.duration_ms)
    except Exception:
        conn.rollback()
        log_error(logger, "OHLCV load failed — rolling back", exc_info=True, step="load", table=table)
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    for idx in INDICES:
        key = idx["key"]
        load(data_path(key, "ohlcv"), bronze_ohlcv(key))
