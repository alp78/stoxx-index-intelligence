"""Gold transform: silver.signals_daily + silver.index_dim -> gold.scores_daily.

Computes per-stock analytics for the latest signal date:
  - Relative value z-scores (within sector, cheap = positive)
  - Momentum signals (relative strength, SMA ratios)
  - Analyst sentiment (implied upside, divergence flags)
  - Composite score and ranks
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from utils.db import get_connection
from utils.config import get_all_keys, silver_ohlcv
from utils.logger import get_logger, log_info, log_warning, log_error, StepTimer
from transforms._gold_utils import zscore_by_group, composite_score, dense_rank_asc

logger = get_logger(__name__)


def run():
    log_info(logger, "Computing daily gold scores (relative value, momentum, sentiment)",
             step="transform", target="gold.scores_daily")

    conn = get_connection()
    cursor = conn.cursor()

    try:
        with StepTimer() as timer:
            # --- 1. Find the latest signal date ---
            cursor.execute("SELECT MAX(signal_date) FROM silver.signals_daily")
            row = cursor.fetchone()
            if not row or not row[0]:
                log_warning(logger, "No data in silver.signals_daily — skipping gold daily scores",
                            step="transform")
                return
            score_date = row[0]

            # --- 2. Pull silver data for that date + sector from index_dim ---
            cursor.execute("""
                SELECT s._index, s.symbol, s.signal_date,
                       s.current_price, s.forward_pe, s.price_to_book,
                       s.ev_to_ebitda, s.dividend_yield, s.market_cap, s.beta,
                       s.fifty_two_week_change, s.sandp_52_week_change,
                       s.fifty_day_average, s.two_hundred_day_average,
                       s.dist_from_52_week_high,
                       s.target_median_price, s.recommendation_mean,
                       s.upside_potential,
                       d.sector, d.short_name, d.country, d.currency
                FROM silver.signals_daily s
                JOIN silver.index_dim d
                    ON s._index = d._index AND s.symbol = d.symbol AND d.is_current = 1
                WHERE s.signal_date = ?
            """, score_date)

            cols = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            if not rows:
                log_warning(logger, "No rows for latest signal date — skipping",
                            step="transform", score_date=str(score_date))
                return

            df = pd.DataFrame.from_records(rows, columns=cols)

            # --- 3. Relative Value z-scores ---
            # Invert PE/PB/EV (lower = cheaper = positive z-score)
            df['pe_zscore'] = -zscore_by_group(df, 'forward_pe', ['_index', 'sector'],
                                               fallback_cols=['_index'])
            df['pb_zscore'] = -zscore_by_group(df, 'price_to_book', ['_index', 'sector'],
                                               fallback_cols=['_index'])
            df['ev_ebitda_zscore'] = -zscore_by_group(df, 'ev_to_ebitda', ['_index', 'sector'],
                                                      fallback_cols=['_index'])
            # Yield: higher = better (no inversion)
            df['yield_zscore'] = zscore_by_group(df, 'dividend_yield', ['_index', 'sector'],
                                                 fallback_cols=['_index'])

            df['relative_value_score'] = composite_score(
                df['pe_zscore'].values, df['pb_zscore'].values,
                df['ev_ebitda_zscore'].values, df['yield_zscore'].values
            )
            df['relative_value_rank'] = df.groupby('_index')['relative_value_score'].transform(
                lambda s: s.rank(method='dense', ascending=False, na_option='keep')
            ).astype('Int16')

            # --- 4. Momentum ---
            df['relative_strength'] = df['fifty_two_week_change'] - df['sandp_52_week_change']

            df['sma_50_ratio'] = np.where(
                df['fifty_day_average'].notna() & (df['fifty_day_average'] != 0),
                df['current_price'] / df['fifty_day_average'], np.nan
            )
            df['sma_200_ratio'] = np.where(
                df['two_hundred_day_average'].notna() & (df['two_hundred_day_average'] != 0),
                df['current_price'] / df['two_hundred_day_average'], np.nan
            )

            # Z-score each momentum metric within _index, then composite
            mom_z_rs = zscore_by_group(df, 'relative_strength', ['_index'])
            mom_z_50 = zscore_by_group(df, 'sma_50_ratio', ['_index'])
            mom_z_200 = zscore_by_group(df, 'sma_200_ratio', ['_index'])
            # dist_from_52w_high is negative (closer to 0 = stronger), invert
            mom_z_high = -zscore_by_group(df, 'dist_from_52_week_high', ['_index'])

            df['momentum_score'] = composite_score(
                mom_z_rs.values, mom_z_50.values, mom_z_200.values, mom_z_high.values
            )
            df['momentum_rank'] = df.groupby('_index')['momentum_score'].transform(
                lambda s: s.rank(method='dense', ascending=False, na_option='keep')
            ).astype('Int16')

            # --- 5. Analyst Sentiment ---
            df['implied_upside'] = np.where(
                df['current_price'].notna() & (df['current_price'] != 0),
                (df['target_median_price'] / df['current_price']) - 1, np.nan
            )

            df['price_falling_analysts_bullish'] = (
                (df['recommendation_mean'] < 2.5) &
                (df['fifty_two_week_change'] < -0.10)
            ).astype('Int16')

            # Composite: z-score upside (higher = better) + inverted recommendation (lower = better)
            sent_z_upside = zscore_by_group(df, 'implied_upside', ['_index'])
            df['_rec_inverted'] = -df['recommendation_mean']
            sent_z_rec = zscore_by_group(df, '_rec_inverted', ['_index'])

            df['sentiment_score'] = composite_score(
                sent_z_upside.values, sent_z_rec.values
            )
            df['sentiment_rank'] = df.groupby('_index')['sentiment_score'].transform(
                lambda s: s.rank(method='dense', ascending=False, na_option='keep')
            ).astype('Int16')

            # --- 6. Composite ---
            df['composite_score'] = composite_score(
                df['relative_value_score'].values,
                df['momentum_score'].values,
                df['sentiment_score'].values
            )
            df['composite_rank'] = df.groupby('_index')['composite_score'].transform(
                lambda s: s.rank(method='dense', ascending=False, na_option='keep')
            ).astype('Int16')

            # --- 7. Index weight (market_cap / sum per index) ---
            df['market_cap'] = pd.to_numeric(df['market_cap'], errors='coerce')
            df['index_weight'] = df.groupby('_index')['market_cap'].transform(
                lambda s: s / s.sum()
            )

            # --- 8. Moving averages + price changes from silver OHLCV ---
            ohlcv_frames = []
            for key in get_all_keys():
                table = silver_ohlcv(key)
                cursor.execute(f"""
                    WITH ranked AS (
                        SELECT symbol, date, [close],
                               ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn,
                               AVG([close]) OVER (PARTITION BY symbol ORDER BY date
                                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS sma_30,
                               AVG([close]) OVER (PARTITION BY symbol ORDER BY date
                                    ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS sma_90,
                               COUNT([close]) OVER (PARTITION BY symbol ORDER BY date
                                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS cnt_30,
                               COUNT([close]) OVER (PARTITION BY symbol ORDER BY date
                                    ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS cnt_90,
                               LAG([close], 1) OVER (PARTITION BY symbol ORDER BY date) AS prev_close,
                               LAG([close], 5) OVER (PARTITION BY symbol ORDER BY date) AS close_5d_ago
                        FROM {table}
                        WHERE [close] IS NOT NULL
                    ),
                    ytd AS (
                        SELECT symbol,
                               MAX(CASE WHEN rn = 1 THEN [close] END) AS latest_close,
                               MAX(CASE WHEN date <= DATEFROMPARTS(YEAR(GETDATE()), 1, 1)
                                    THEN date END) AS ytd_date
                        FROM ranked
                        GROUP BY symbol
                    ),
                    ytd_price AS (
                        SELECT y.symbol, r.[close] AS ytd_close
                        FROM ytd y
                        JOIN ranked r ON y.symbol = r.symbol AND r.date = y.ytd_date
                    )
                    SELECT r.symbol,
                           CASE WHEN cnt_30 >= 30 THEN sma_30 END AS sma_30_close,
                           CASE WHEN cnt_90 >= 90 THEN sma_90 END AS sma_90_close,
                           CASE WHEN prev_close > 0
                                THEN (r.[close] - prev_close) / prev_close END AS day_change_pct,
                           CASE WHEN close_5d_ago > 0
                                THEN (r.[close] - close_5d_ago) / close_5d_ago END AS five_day_change_pct,
                           CASE WHEN yp.ytd_close > 0
                                THEN (r.[close] - yp.ytd_close) / yp.ytd_close END AS ytd_change_pct
                    FROM ranked r
                    LEFT JOIN ytd_price yp ON r.symbol = yp.symbol
                    WHERE r.rn = 1
                """)
                cols_ohlcv = [desc[0] for desc in cursor.description]
                rows_ohlcv = cursor.fetchall()
                if rows_ohlcv:
                    ohlcv_df = pd.DataFrame.from_records(rows_ohlcv, columns=cols_ohlcv)
                    ohlcv_df['_index'] = key
                    ohlcv_frames.append(ohlcv_df)

            if ohlcv_frames:
                ohlcv_all = pd.concat(ohlcv_frames, ignore_index=True)
                df = df.merge(ohlcv_all, on=['_index', 'symbol'], how='left')
            else:
                df['sma_30_close'] = np.nan
                df['sma_90_close'] = np.nan
                df['day_change_pct'] = np.nan
                df['five_day_change_pct'] = np.nan
                df['ytd_change_pct'] = np.nan

            # --- 9. Write to gold ---
            cursor.execute("DELETE FROM gold.scores_daily WHERE score_date = ?", score_date)
            deleted = cursor.rowcount

            insert_cols = [
                '_index', 'symbol', 'score_date', 'sector',
                'pe_zscore', 'pb_zscore', 'ev_ebitda_zscore', 'yield_zscore',
                'relative_value_score', 'relative_value_rank',
                'relative_strength', 'sma_50_ratio', 'sma_200_ratio', 'dist_from_52_week_high',
                'momentum_score', 'momentum_rank',
                'implied_upside', 'recommendation_mean', 'price_falling_analysts_bullish',
                'sentiment_score', 'sentiment_rank',
                'composite_score', 'composite_rank',
                'sma_30_close', 'sma_90_close',
                'market_cap', 'index_weight',
                'short_name', 'country', 'current_price',
                'day_change_pct', 'five_day_change_pct', 'ytd_change_pct',
                'currency',
            ]

            # Rename signal_date -> score_date for insert
            df['score_date'] = df['signal_date']
            # Replace pandas NaN/NA with None for pyodbc
            insert_df = df[insert_cols].where(df[insert_cols].notna(), None)

            # Map DataFrame column to DB column name
            col_map = {'dist_from_52_week_high': 'dist_from_52w_high'}
            db_cols = [col_map.get(c, c) for c in insert_cols]
            placeholders = ', '.join(['?'] * len(insert_cols))
            col_list = ', '.join(db_cols)

            def _clean(v):
                if v is None or v is pd.NA or (isinstance(v, float) and np.isnan(v)):
                    return None
                if hasattr(v, 'item'):
                    return v.item()
                return v

            rows = [tuple(_clean(v) for v in row) for row in insert_df.itertuples(index=False, name=None)]
            cursor.fast_executemany = True
            cursor.executemany(
                f"INSERT INTO gold.scores_daily ({col_list}) VALUES ({placeholders})",
                rows
            )
            inserted = len(rows)
            conn.commit()

        log_info(logger, "Daily gold scores computed — relative value, momentum, sentiment ranked",
                 step="transform", target="gold.scores_daily",
                 score_date=str(score_date), records_inserted=inserted,
                 records_replaced=deleted, duration_ms=timer.duration_ms)
    except Exception:
        conn.rollback()
        log_error(logger, "Daily gold scores transform failed", exc_info=True,
                  step="transform", target="gold.scores_daily")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    run()
