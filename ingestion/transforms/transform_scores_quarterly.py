"""Gold transform: silver.signals_quarterly + silver.index_dim -> gold.scores_quarterly.

Computes per-stock analytics for the latest quarterly data:
  - Quality / moat z-scores (margins, ROE, leverage, FCF yield)
  - Financial health risk flags (rules-based thresholds)
  - Governance composite (ISS risk scores)
  - Cross-domain comparison (governance vs quality gap)
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from utils.db import get_connection
from utils.logger import get_logger, log_info, log_warning, log_error, StepTimer
from transforms._gold_utils import zscore_by_group, composite_score

logger = get_logger(__name__)

_RISK_LEVELS = {0: 'healthy', 1: 'watch', 2: 'warning', 3: 'critical', 4: 'critical'}


def run():
    log_info(logger, "Computing quarterly gold scores (quality, health flags, governance)",
             step="transform", target="gold.scores_quarterly")

    conn = get_connection()
    cursor = conn.cursor()

    try:
        with StepTimer() as timer:
            # --- 1. Get latest as_of_date per (_index, symbol) ---
            cursor.execute("""
                SELECT q._index, q.symbol, q.as_of_date,
                       q.gross_margins, q.operating_margins, q.return_on_equity,
                       q.revenue_growth, q.earnings_growth,
                       q.debt_to_equity, q.current_ratio, q.free_cashflow,
                       q.overall_risk, q.audit_risk, q.board_risk,
                       q.compensation_risk, q.shareholder_rights_risk,
                       d.sector
                FROM silver.signals_quarterly q
                JOIN silver.index_dim d
                    ON q._index = d._index AND q.symbol = d.symbol AND d.is_current = 1
                WHERE q.as_of_date = (
                    SELECT MAX(q2.as_of_date)
                    FROM silver.signals_quarterly q2
                    WHERE q2._index = q._index AND q2.symbol = q.symbol
                )
            """)
            cols = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            if not rows:
                log_warning(logger, "No data in silver.signals_quarterly — skipping gold quarterly scores",
                            step="transform")
                return

            df = pd.DataFrame.from_records(rows, columns=cols)

            # --- 2. Get latest market_cap + beta from silver.signals_daily ---
            cursor.execute("""
                SELECT s._index, s.symbol, s.market_cap, s.beta
                FROM silver.signals_daily s
                WHERE s.signal_date = (
                    SELECT MAX(s2.signal_date)
                    FROM silver.signals_daily s2
                    WHERE s2._index = s._index AND s2.symbol = s.symbol
                )
            """)
            daily_cols = [desc[0] for desc in cursor.description]
            daily_rows = cursor.fetchall()
            daily_df = pd.DataFrame.from_records(daily_rows, columns=daily_cols)

            if not daily_df.empty:
                df = df.merge(daily_df, on=['_index', 'symbol'], how='left')
            else:
                df['market_cap'] = np.nan
                df['beta'] = np.nan

            # --- 3. Quality / Moat z-scores ---
            # FCF yield = free_cashflow / market_cap
            df['fcf_yield'] = np.where(
                df['market_cap'].notna() & (df['market_cap'] != 0),
                df['free_cashflow'] / df['market_cap'], np.nan
            )

            # Higher = better for margins, ROE, FCF yield
            df['gross_margin_zscore'] = zscore_by_group(df, 'gross_margins', ['_index', 'sector'],
                                                        fallback_cols=['_index'])
            df['roe_zscore'] = zscore_by_group(df, 'return_on_equity', ['_index', 'sector'],
                                               fallback_cols=['_index'])
            df['operating_margin_zscore'] = zscore_by_group(df, 'operating_margins', ['_index', 'sector'],
                                                            fallback_cols=['_index'])
            df['fcf_yield_zscore'] = zscore_by_group(df, 'fcf_yield', ['_index', 'sector'],
                                                     fallback_cols=['_index'])

            # Lower D/E = better → invert
            df['leverage_zscore'] = -zscore_by_group(df, 'debt_to_equity', ['_index', 'sector'],
                                                     fallback_cols=['_index'])

            df['quality_score'] = composite_score(
                df['gross_margin_zscore'].values, df['roe_zscore'].values,
                df['operating_margin_zscore'].values, df['leverage_zscore'].values,
                df['fcf_yield_zscore'].values
            )
            df['quality_rank'] = df.groupby('_index')['quality_score'].transform(
                lambda s: s.rank(method='dense', ascending=False, na_option='keep')
            ).astype('Int16')

            # --- 4. Financial Health Flags ---
            df['flag_liquidity'] = (df['current_ratio'].notna() & (df['current_ratio'] < 1)).astype('Int16')
            df['flag_leverage'] = (df['debt_to_equity'].notna() & (df['debt_to_equity'] > 200)).astype('Int16')
            df['flag_cashburn'] = (df['free_cashflow'].notna() & (df['free_cashflow'] < 0)).astype('Int16')
            df['flag_double_decline'] = (
                df['earnings_growth'].notna() & (df['earnings_growth'] < 0) &
                df['revenue_growth'].notna() & (df['revenue_growth'] < 0)
            ).astype('Int16')

            df['health_flags_count'] = (
                df['flag_liquidity'].fillna(0) + df['flag_leverage'].fillna(0) +
                df['flag_cashburn'].fillna(0) + df['flag_double_decline'].fillna(0)
            ).astype('Int16')

            df['health_risk_level'] = df['health_flags_count'].map(_RISK_LEVELS)

            # --- 5. Governance ---
            risk_cols = ['overall_risk', 'audit_risk', 'board_risk',
                         'compensation_risk', 'shareholder_rights_risk']

            # governance_score = 10 - avg(risk scores), higher = better governance
            risk_values = df[risk_cols].values.astype(float)
            with np.errstate(invalid='ignore'):
                avg_risk = np.nanmean(risk_values, axis=1)
            df['governance_score'] = np.where(np.isnan(avg_risk), np.nan, 10.0 - avg_risk)

            df['governance_rank'] = df.groupby('_index')['governance_score'].transform(
                lambda s: s.rank(method='dense', ascending=False, na_option='keep')
            ).astype('Int16')

            # governance vs quality gap (positive = governance outpaces fundamentals)
            df['governance_vs_quality'] = df['governance_score'] - df['quality_score']

            # --- 6. Write to gold ---
            # Get the distinct as_of_dates we're inserting
            as_of_dates = df['as_of_date'].dropna().unique()
            for aod in as_of_dates:
                cursor.execute("DELETE FROM gold.scores_quarterly WHERE as_of_date = ?", aod)
            deleted = cursor.rowcount

            insert_cols = [
                '_index', 'symbol', 'as_of_date', 'sector',
                'gross_margin_zscore', 'roe_zscore', 'operating_margin_zscore',
                'leverage_zscore', 'fcf_yield', 'fcf_yield_zscore',
                'quality_score', 'quality_rank',
                'flag_liquidity', 'flag_leverage', 'flag_cashburn', 'flag_double_decline',
                'health_flags_count', 'health_risk_level',
                'overall_risk', 'audit_risk', 'board_risk',
                'compensation_risk', 'shareholder_rights_risk',
                'governance_score', 'governance_rank',
                'beta', 'governance_vs_quality',
            ]

            insert_df = df[insert_cols].where(df[insert_cols].notna(), None)
            placeholders = ', '.join(['?'] * len(insert_cols))
            col_list = ', '.join(insert_cols)

            def _clean(v):
                if v is None or v is pd.NA or (isinstance(v, float) and np.isnan(v)):
                    return None
                if hasattr(v, 'item'):
                    return v.item()
                return v

            rows = [tuple(_clean(v) for v in row) for row in insert_df.itertuples(index=False, name=None)]
            cursor.fast_executemany = True
            cursor.executemany(
                f"INSERT INTO gold.scores_quarterly ({col_list}) VALUES ({placeholders})",
                rows
            )
            inserted = len(rows)
            conn.commit()

        log_info(logger, "Quarterly gold scores computed — quality, health flags, governance ranked",
                 step="transform", target="gold.scores_quarterly",
                 records_inserted=inserted, records_replaced=deleted,
                 duration_ms=timer.duration_ms)
    except Exception:
        conn.rollback()
        log_error(logger, "Quarterly gold scores transform failed", exc_info=True,
                  step="transform", target="gold.scores_quarterly")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    run()
