"""Gap-fill transform: bronze OHLCV -> silver OHLCV (per index).
Uses bronze.trading_calendar per-exchange to identify missing trading days,
then forward-fills. Each symbol uses its own exchange's calendar."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import get_connection
from config import get_all_keys, bronze_ohlcv, silver_ohlcv


def run():
    conn = get_connection()
    cursor = conn.cursor()

    for key in get_all_keys():
        print(f"\n--- {key} ---")
        _transform_index(cursor, conn, key, bronze_ohlcv(key), silver_ohlcv(key))

    cursor.close()
    conn.close()


def _transform_index(cursor, conn, index_name, bronze_table, silver_table):

    # Get symbol -> exchange mapping from bronze.index_dim
    cursor.execute("""
        SELECT symbol, exchange FROM bronze.index_dim WHERE _index = ?
    """, index_name)
    symbol_exchange = {r[0]: r[1] for r in cursor.fetchall()}

    if not symbol_exchange:
        print("  No symbols found in bronze.index_dim. Run load_index_dim first.")
        return

    print(f"  Symbols: {len(symbol_exchange)}")

    # Pre-load trading calendars per exchange (exchange_code -> sorted list of dates)
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
        print("  No trading days found in calendar. Run transform_trading_calendar first.")
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
            print(f"  Warning: No calendar for exchange '{exchange}' (symbol {symbol}), skipping")
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
    print(f"  Inserted: {inserted} actual, {filled} forward-filled")


if __name__ == "__main__":
    run()
