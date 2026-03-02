"""Populates bronze.trading_calendar from exchange_calendars library.
Covers all exchanges found in bronze.index_dim. Truncate & reload."""

import sys
from pathlib import Path
from datetime import date

import exchange_calendars as xcals

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import get_connection

# Non-obvious yfinance -> exchange_calendars mappings.
# Most exchanges follow the X{code} pattern (e.g. AMS -> XAMS).
# Only exceptions need to be listed here.
_EXCEPTIONS = {
    "GER": "XFRA",   # Germany -> Frankfurt
    "MCE": "XMAD",   # Madrid
    "NMS": "XNYS",   # NASDAQ -> NYSE calendar
    "NYQ": "XNYS",   # NYSE
    "JPX": "XTKS",   # Japan Exchange -> Tokyo
}

_VALID_CALENDARS = set(xcals.get_calendar_names())


def _resolve_xc_code(yf_code):
    """Auto-resolve yfinance exchange code to exchange_calendars code."""
    if yf_code in _EXCEPTIONS:
        return _EXCEPTIONS[yf_code]
    candidate = f"X{yf_code}"
    if candidate in _VALID_CALENDARS:
        return candidate
    return None

# Range: from earliest price_data_start to 5 years from now
START_FALLBACK = "2015-01-01"
FUTURE_YEARS = 5


def run():
    conn = get_connection()
    cursor = conn.cursor()

    # Get date range from bronze dimensions
    cursor.execute("""
        SELECT MIN(price_data_start) FROM bronze.index_dim
        WHERE price_data_start IS NOT NULL
    """)
    row = cursor.fetchone()
    start = str(row[0]) if row[0] else START_FALLBACK
    end = date(date.today().year + FUTURE_YEARS, 12, 31).isoformat()

    print(f"Calendar range: {start} to {end}")

    # Get distinct exchanges actually used
    cursor.execute("SELECT DISTINCT exchange FROM bronze.index_dim")
    used_exchanges = [r[0] for r in cursor.fetchall()]

    # Truncate & reload
    cursor.execute("DELETE FROM bronze.trading_calendar")
    conn.commit()

    total = 0

    for yf_code in used_exchanges:
        xc_code = _resolve_xc_code(yf_code)
        if not xc_code:
            print(f"  Warning: Cannot resolve exchange '{yf_code}' (tried X{yf_code}), skipping")
            continue

        print(f"  {yf_code} -> {xc_code}...", end=" ")

        cal = xcals.get_calendar(xc_code)
        # Clamp end to calendar's last available session
        cal_end = min(end, str(cal.last_session.date()))
        sessions = cal.sessions_in_range(start, cal_end)
        trading_dates = set(sessions.date)

        # Generate all calendar days in range
        all_days = []
        d = date.fromisoformat(start)
        end_d = date.fromisoformat(cal_end)
        while d <= end_d:
            all_days.append(d)
            d = date(d.year, d.month, d.day + 1) if d.day < 28 else _next_day(d)

        # Build rows
        rows = []
        for d in all_days:
            is_trading = d in trading_dates
            rows.append((
                d.isoformat(),
                yf_code,
                xc_code,
                d.year,
                (d.month - 1) // 3 + 1,   # quarter
                d.month,
                d.isocalendar()[1],         # week_of_year
                d.weekday(),               # 0=Mon
                1 if is_trading else 0,
                0,  # is_month_end (filled below)
                0,  # is_quarter_end (filled below)
            ))

        # Mark last trading day of each month and quarter
        trading_rows = [(i, r) for i, r in enumerate(rows) if r[8] == 1]
        last_by_month = {}
        last_by_quarter = {}
        for i, r in trading_rows:
            key_m = (r[3], r[5])           # (year, month)
            key_q = (r[3], r[4])           # (year, quarter)
            last_by_month[key_m] = i
            last_by_quarter[key_q] = i

        rows_final = []
        month_end_indices = set(last_by_month.values())
        quarter_end_indices = set(last_by_quarter.values())
        for i, r in enumerate(rows):
            rows_final.append(r[:9] + (
                1 if i in month_end_indices else 0,
                1 if i in quarter_end_indices else 0,
            ))

        # Bulk insert
        for r in rows_final:
            cursor.execute("""
                INSERT INTO bronze.trading_calendar
                    (date, exchange_code, xc_code, year, quarter, month,
                     week_of_year, day_of_week, is_trading_day, is_month_end, is_quarter_end)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, *r)

        conn.commit()
        count = len(rows_final)
        total += count
        print(f"{count} rows")

    cursor.close()
    conn.close()
    print(f"\nDone: {total} total calendar rows inserted")


def _next_day(d):
    """Simple next-day helper to avoid timedelta import."""
    from datetime import timedelta
    return d + timedelta(days=1)


if __name__ == "__main__":
    run()
