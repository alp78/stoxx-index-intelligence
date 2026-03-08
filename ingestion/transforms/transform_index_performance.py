"""Gold transform: silver OHLCV + silver.signals_daily -> gold.index_performance.

Computes per-index daily time series:
  - Market-cap-weighted daily price return (close, not [close]) across all stocks
  - Cumulative return factor (rebased from first trading day)
  - Rolling 30d/90d returns and annualized volatility
  - YTD return
  - Cap-weighted cross-sectional aggregates (PE, PB, yield, market cap)

Strategy: incremental — only insert dates newer than the latest existing row.
"""

import sys
from math import sqrt
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from utils.db import get_connection
from utils.config import get_all_keys, silver_ohlcv
from utils.logger import get_logger, log_info, log_warning, log_error, StepTimer

logger = get_logger(__name__)

_ANNUALIZATION = sqrt(252)


def run():
    log_info(logger, "Computing cap-weighted index performance time series",
             step="transform", target="gold.index_performance")

    conn = get_connection()
    cursor = conn.cursor()

    try:
        with StepTimer() as timer:
            total_inserted = 0

            for key in get_all_keys():
                inserted = _transform_index(cursor, conn, key)
                total_inserted += inserted

            conn.commit()

        log_info(logger, "Index performance transform complete",
                 step="transform", target="gold.index_performance",
                 records_inserted=total_inserted, duration_ms=timer.duration_ms)
    except Exception:
        conn.rollback()
        log_error(logger, "Index performance transform failed", exc_info=True,
                  step="transform", target="gold.index_performance")
        raise
    finally:
        cursor.close()
        conn.close()


def _transform_index(cursor, conn, key):
    """Compute and insert cap-weighted index performance rows for a single index."""
    table = silver_ohlcv(key)

    # Check if the silver OHLCV table exists
    schema, tbl = table.split('.')
    cursor.execute("""
        SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = ? AND t.name = ?
    """, schema, tbl)
    if not cursor.fetchone():
        log_warning(logger, "Silver OHLCV table does not exist — skipping",
                    step="transform", index=key, table=table)
        return 0

    # Find the latest date already in gold for this index (for incremental)
    cursor.execute("""
        SELECT MAX(perf_date) FROM gold.index_performance WHERE _index = ?
    """, key)
    row = cursor.fetchone()
    max_existing = row[0] if row and row[0] else None

    # --- 1. Pull all OHLCV ([close]) from silver ---
    cursor.execute(f"""
        SELECT symbol, date, [close]
        FROM {table}
        WHERE [close] IS NOT NULL
        ORDER BY symbol, date
    """)
    ohlcv_rows = [tuple(r) for r in cursor.fetchall()]

    if not ohlcv_rows:
        log_warning(logger, "No OHLCV data in silver — skipping",
                    step="transform", index=key, table=table)
        return 0

    prices = pd.DataFrame(ohlcv_rows, columns=['symbol', 'date', 'close'])
    prices['date'] = pd.to_datetime(prices['date'])

    # --- 2. Compute daily returns per stock ---
    prices = prices.sort_values(['symbol', 'date'])
    prices['daily_ret'] = prices.groupby('symbol')['close'].pct_change()

    # --- 3. Pull market cap + fundamentals from silver.signals_daily ---
    cursor.execute("""
        SELECT symbol, CAST(signal_date AS DATE) AS sig_date,
               market_cap, forward_pe, price_to_book,
               dividend_yield
        FROM silver.signals_daily
        WHERE _index = ? AND market_cap IS NOT NULL AND market_cap > 0
        ORDER BY symbol, signal_date
    """, key)
    sig_rows = [tuple(r) for r in cursor.fetchall()]

    if sig_rows:
        signals = pd.DataFrame(sig_rows, columns=['symbol', 'date', 'market_cap',
                                                   'forward_pe', 'price_to_book',
                                                   'dividend_yield'])
        signals['date'] = pd.to_datetime(signals['date'])
        signals['market_cap'] = signals['market_cap'].astype(float)
    else:
        # Fallback: no signals available, use equal weights
        log_warning(logger, "No market cap data in signals — falling back to equal weights",
                    step="transform", index=key)
        signals = None

    # --- 4. Merge market cap onto prices for weighting ---
    if signals is not None:
        # Keep only market_cap for the merge (fundamentals handled separately)
        mcap = signals[['symbol', 'date', 'market_cap']].copy()

        # merge_asof: for each (symbol, date) in prices, find nearest signal date
        prices = prices.sort_values(['symbol', 'date'])
        mcap = mcap.sort_values(['symbol', 'date'])

        merged = []
        for sym in prices['symbol'].unique():
            sym_prices = prices[prices['symbol'] == sym].copy()
            sym_mcap = mcap[mcap['symbol'] == sym].copy()
            if sym_mcap.empty:
                sym_prices['market_cap'] = np.nan
            else:
                sym_prices = pd.merge_asof(
                    sym_prices, sym_mcap[['date', 'market_cap']],
                    on='date', direction='backward',
                    tolerance=pd.Timedelta('7D')
                )
            merged.append(sym_prices)

        prices = pd.concat(merged, ignore_index=True)
    else:
        prices['market_cap'] = 1.0  # equal weight fallback

    # --- 5. Cap-weighted daily index return per date ---
    # Drop rows without a return (first day per stock) or without market cap
    ret_df = prices.dropna(subset=['daily_ret']).copy()

    def _cap_weighted_return(group):
        mcaps = group['market_cap']
        rets = group['daily_ret']
        valid = mcaps.notna() & (mcaps > 0)
        if valid.sum() == 0:
            # fallback to equal weight for this date
            return pd.Series({
                'daily_return': rets.mean(),
                'stocks_count': len(rets),
            })
        w = mcaps[valid]
        r = rets[valid]
        total_w = w.sum()
        weighted_ret = (r * w).sum() / total_w
        return pd.Series({
            'daily_return': weighted_ret,
            'stocks_count': valid.sum(),
        })

    idx_ret = ret_df.groupby('date').apply(_cap_weighted_return).reset_index()
    idx_ret['stocks_count'] = idx_ret['stocks_count'].astype(int)
    idx_ret = idx_ret.sort_values('date')

    if idx_ret.empty:
        return 0

    # --- 6. Cumulative factor ---
    idx_ret['cumulative_factor'] = (1 + idx_ret['daily_return']).cumprod()

    # --- 7. Rolling returns ---
    idx_ret['rolling_30d_return'] = (
        (1 + idx_ret['daily_return']).rolling(30, min_periods=30).apply(np.prod, raw=True) - 1
    )
    idx_ret['rolling_90d_return'] = (
        (1 + idx_ret['daily_return']).rolling(90, min_periods=90).apply(np.prod, raw=True) - 1
    )

    # --- 8. YTD return ---
    idx_ret['year'] = idx_ret['date'].dt.year
    ytd_returns = []
    for _, year_df in idx_ret.groupby('year'):
        ytd = (1 + year_df['daily_return']).cumprod() - 1
        ytd_returns.append(ytd)
    idx_ret['ytd_return'] = pd.concat(ytd_returns)
    idx_ret.drop(columns=['year'], inplace=True)

    # --- 9. Rolling 30d volatility (annualized) ---
    idx_ret['rolling_30d_volatility'] = (
        idx_ret['daily_return'].rolling(30, min_periods=30).std() * _ANNUALIZATION
    )

    # --- 10. Cap-weighted cross-sectional aggregates ---
    if signals is not None:
        # Clamp dividend_yield to [0, 0.20] — yfinance occasionally returns
        # outlier values that skew the cap-weighted average.
        signals['dividend_yield'] = signals['dividend_yield'].clip(upper=0.20)

        def _cap_weighted_aggs(group):
            mcaps = group['market_cap']
            valid = mcaps.notna() & (mcaps > 0)
            if valid.sum() == 0:
                return pd.Series({
                    'avg_pe': np.nan, 'avg_pb': np.nan,
                    'avg_dividend_yield': np.nan, 'avg_market_cap': np.nan,
                })
            w = mcaps[valid]
            total_w = w.sum()

            def _wmean(col):
                vals = group.loc[valid, col]
                mask = vals.notna()
                if mask.sum() == 0:
                    return np.nan
                return (vals[mask] * w[mask]).sum() / w[mask].sum()

            return pd.Series({
                'avg_pe': _wmean('forward_pe'),
                'avg_pb': _wmean('price_to_book'),
                'avg_dividend_yield': _wmean('dividend_yield'),
                'avg_market_cap': w.mean(),
            })

        agg_df = signals.groupby('date').apply(_cap_weighted_aggs).reset_index()
        agg_df = agg_df.sort_values('date')

        idx_ret = pd.merge_asof(
            idx_ret.sort_values('date'),
            agg_df,
            on='date',
            direction='nearest',
            tolerance=pd.Timedelta('3D')
        )
    else:
        idx_ret['avg_pe'] = np.nan
        idx_ret['avg_pb'] = np.nan
        idx_ret['avg_dividend_yield'] = np.nan
        idx_ret['avg_market_cap'] = np.nan

    # Forward-fill aggregates (signals are daily snapshots, OHLCV has more dates)
    for col in ['avg_pe', 'avg_pb', 'avg_dividend_yield', 'avg_market_cap']:
        idx_ret[col] = idx_ret[col].ffill()

    # --- 11. Filter to only new dates (incremental) ---
    # Always refresh the last 7 days so late-arriving signals (PE, PB, yield)
    # get merged onto existing OHLCV dates.
    if max_existing:
        max_existing_dt = pd.Timestamp(max_existing)
        refresh_from = max_existing_dt - pd.Timedelta('7D')
        cursor.execute("""
            DELETE FROM gold.index_performance
            WHERE _index = ? AND perf_date >= ?
        """, key, refresh_from.strftime('%Y-%m-%d'))
        idx_ret = idx_ret[idx_ret['date'] >= refresh_from]

    if idx_ret.empty:
        log_info(logger, "Index performance already up to date — no new dates",
                 step="transform", index=key)
        return 0

    # --- 12. Insert into gold ---
    idx_ret['_index'] = key

    insert_cols = [
        '_index', 'perf_date', 'daily_return', 'cumulative_factor',
        'rolling_30d_return', 'rolling_90d_return', 'ytd_return',
        'rolling_30d_volatility', 'stocks_count',
        'avg_pe', 'avg_pb', 'avg_dividend_yield', 'avg_market_cap',
    ]

    placeholders = ', '.join(['?'] * len(insert_cols))
    col_list = ', '.join(insert_cols)

    rows = []
    for _, row in idx_ret.iterrows():
        rows.append((
            key,
            row['date'].strftime('%Y-%m-%d'),
            _safe(row['daily_return']),
            _safe(row['cumulative_factor']),
            _safe(row['rolling_30d_return']),
            _safe(row['rolling_90d_return']),
            _safe(row['ytd_return']),
            _safe(row['rolling_30d_volatility']),
            int(row['stocks_count']) if pd.notna(row['stocks_count']) else None,
            _safe(row['avg_pe']),
            _safe(row['avg_pb']),
            _safe(row['avg_dividend_yield']),
            int(row['avg_market_cap']) if pd.notna(row['avg_market_cap']) else None,
        ))

    cursor.fast_executemany = True
    cursor.executemany(
        f"INSERT INTO gold.index_performance ({col_list}) VALUES ({placeholders})",
        rows
    )
    inserted = len(rows)
    conn.commit()

    log_info(logger, "Index performance rows inserted",
             step="transform", index=key, records_inserted=inserted)

    return inserted


def _safe(val):
    """Convert pandas NaN/NaT to None for pyodbc."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    return float(val)


if __name__ == "__main__":
    run()
