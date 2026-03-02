"""Gap-fill transform: bronze OHLCV -> silver OHLCV (per index).
Uses bronze.trading_calendar per-exchange to identify missing trading days,
then forward-fills. Each symbol uses its own exchange's calendar."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import get_connection
from config import get_all_keys, bronze_ohlcv, silver_ohlcv
from logger import get_logger, log_info, log_warning, log_error, StepTimer

logger = get_logger(__name__)


def run():
    conn = get_connection()
    cursor = conn.cursor()

    for key in get_all_keys():
        _transform_index(cursor, conn, key, bronze_ohlcv(key), silver_ohlcv(key))

    cursor.close()
    conn.close()


def _transform_index(cursor, conn, index_name, bronze_table, silver_table):
    log_info(logger, "Transform started", step="transform",
             index=index_name, source=bronze_table, target=silver_table)

    try:
        with StepTimer() as timer:
            # Get symbol -> exchange mapping from bronze.index_dim
            cursor.execute("""
                SELECT symbol, exchange FROM bronze.index_dim WHERE _index = ?
            """, index_name)
            symbol_exchange = {r[0]: r[1] for r in cursor.fetchall()}

            if not symbol_exchange:
                log_warning(logger, "No symbols in bronze.index_dim",
                            step="transform", index=index_name)
                return

            # Pre-load trading calendars per exchange
            exchanges = set(symbol_exchange.values())
            cal_by_exchange = {}
            for exc in exchanges:
                cursor.execute("""
                    SELECT date FROM bronze.trading_calendar
                    WHERE exchange_code = ? AND is_trading_day = 1
                    ORDER BY date
                """, exc)
                cal_by_exchange[exc] = [r[0] for r in cursor.fetchall()]

            if not any(cal_by_exchange.values()):
                log_warning(logger, "No trading days in calendar",
                            step="transform", index=index_name)
                return

            # Get existing silver (symbol, date) to skip
            cursor.execute(f"""
                SELECT symbol, CONVERT(VARCHAR(10), date, 120) FROM {silver_table}
            """)
            existing = set((r[0], r[1]) for r in cursor.fetchall())

            inserted = 0
            filled = 0

            for symbol, exchange in symbol_exchange.items():
                trading_days = cal_by_exchange.get(exchange, [])
                if not trading_days:
                    log_warning(logger, "No calendar for exchange",
                                step="transform", symbol=symbol, exchange=exchange)
                    continue

                # Load bronze data for this symbol
                cursor.execute(f"""
                    SELECT date, [open], high, low, [close], adj_close, volume, dividends, stock_splits
                    FROM {bronze_table}
                    WHERE symbol = ?
                    ORDER BY date
                """, symbol)
                bronze_rows = {str(r[0]): r for r in cursor.fetchall()}

                if not bronze_rows:
                    continue

                first_date = min(bronze_rows.keys())
                last_fill = None

                for td in trading_days:
                    td_str = str(td)
                    if td_str < first_date:
                        continue

                    if (symbol, td_str) in existing:
                        if td_str in bronze_rows:
                            last_fill = bronze_rows[td_str]
                        continue

                    if td_str in bronze_rows:
                        r = bronze_rows[td_str]
                        cursor.execute(f"""
                            INSERT INTO {silver_table}
                                (symbol, date, [open], high, low, [close], adj_close,
                                 volume, dividends, stock_splits, is_filled)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                        """, symbol, td_str, r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8])
                        last_fill = r
                        inserted += 1
                    elif last_fill is not None:
                        close = last_fill[4]
                        adj = last_fill[5]
                        cursor.execute(f"""
                            INSERT INTO {silver_table}
                                (symbol, date, [open], high, low, [close], adj_close,
                                 volume, dividends, stock_splits, is_filled)
                            VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 1)
                        """, symbol, td_str, close, close, close, close, adj)
                        filled += 1

                if (inserted + filled) % 5000 == 0 and (inserted + filled) > 0:
                    conn.commit()

            conn.commit()

        log_info(logger, "Transform complete", step="transform",
                 index=index_name, target=silver_table,
                 records_inserted=inserted, records_filled=filled,
                 symbols=len(symbol_exchange), duration_ms=timer.duration_ms)
    except Exception:
        log_error(logger, "Transform failed", exc_info=True,
                  step="transform", index=index_name, target=silver_table)
        raise


if __name__ == "__main__":
    run()
