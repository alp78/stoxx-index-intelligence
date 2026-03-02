"""Populates bronze.trading_calendar from exchange_calendars library.
Covers all exchanges found in bronze.index_dim.
Incremental: only adds exchanges not already present in the table."""

import sys
from pathlib import Path
from datetime import date, timedelta

import exchange_calendars as xcals

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from utils.db import get_connection
from utils.logger import get_logger, log_info, log_warning, log_error, StepTimer

logger = get_logger(__name__)

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
    with StepTimer() as timer:
        conn = get_connection()
        cursor = conn.cursor()

        try:
            # Get date range from bronze dimensions
            cursor.execute("""
                SELECT MIN(price_data_start) FROM bronze.index_dim
                WHERE price_data_start IS NOT NULL
            """)
            row = cursor.fetchone()
            start = str(row[0]) if row[0] else START_FALLBACK
            end = date(date.today().year + FUTURE_YEARS, 12, 31).isoformat()

            log_info(logger, "Building trading calendar from exchange_calendars library (incremental — new exchanges only)",
                     step="transform", target="bronze.trading_calendar",
                     range_start=start, range_end=end)

            # Get distinct exchanges actually used
            cursor.execute("SELECT DISTINCT exchange FROM bronze.index_dim")
            used_exchanges = [r[0] for r in cursor.fetchall()]

            # Skip exchanges already in the calendar
            cursor.execute("SELECT DISTINCT exchange_code FROM bronze.trading_calendar")
            existing_exchanges = {r[0] for r in cursor.fetchall()}

            # Check for exchanges that need date range extension
            cursor.execute("""
                SELECT exchange_code, MIN(date), MAX(date)
                FROM bronze.trading_calendar
                GROUP BY exchange_code
            """)
            existing_ranges = {r[0]: (str(r[1]), str(r[2])) for r in cursor.fetchall()}

            new_exchanges = [e for e in used_exchanges if e not in existing_exchanges]
            extend_exchanges = []
            for e in used_exchanges:
                if e in existing_ranges:
                    cal_start, cal_end_existing = existing_ranges[e]
                    if start < cal_start or end > cal_end_existing:
                        extend_exchanges.append(e)

            if not new_exchanges and not extend_exchanges:
                log_info(logger, "Trading calendar already covers all exchanges — nothing to add",
                         step="transform", target="bronze.trading_calendar",
                         exchanges=len(used_exchanges))
                return

            total = 0

            for yf_code in new_exchanges + extend_exchanges:
                xc_code = _resolve_xc_code(yf_code)
                if not xc_code:
                    log_warning(logger, "Skipping exchange — cannot map yfinance code to exchange_calendars",
                                step="transform", exchange=yf_code, tried=f"X{yf_code}")
                    continue

                # For existing exchanges being extended, delete and rebuild
                if yf_code in extend_exchanges:
                    cursor.execute("DELETE FROM bronze.trading_calendar WHERE exchange_code = ?", yf_code)
                    conn.commit()

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
                    d += timedelta(days=1)

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
                action = "Extended" if yf_code in extend_exchanges else "Added"
                log_info(logger, f"{action} trading calendar for exchange — all dates from start to end of range",
                         step="transform", exchange=yf_code, xc_code=xc_code, rows=count)

        except Exception:
            conn.rollback()
            log_error(logger, "Trading calendar update failed", exc_info=True,
                      step="transform", target="bronze.trading_calendar")
            raise
        finally:
            cursor.close()
            conn.close()

    log_info(logger, "Trading calendar update complete — new exchange schedules added to bronze",
             step="transform", target="bronze.trading_calendar",
             records_inserted=total, new_exchanges=len(new_exchanges),
             extended_exchanges=len(extend_exchanges),
             existing_exchanges=len(existing_exchanges),
             duration_ms=timer.duration_ms)


if __name__ == "__main__":
    run()
