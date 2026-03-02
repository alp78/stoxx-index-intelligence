"""Shared helpers for gold layer transforms."""

import numpy as np
import pandas as pd


def zscore_by_group(df, col, group_cols, min_peers=3, fallback_cols=None):
    """Z-score within group, with optional fallback to a broader group.

    Args:
        df: DataFrame containing the data.
        col: Column name to z-score.
        group_cols: Columns defining the primary group (e.g. ['_index', 'sector']).
        min_peers: Minimum non-null values in a group to compute z-scores.
                   Groups with fewer peers get NaN (or fallback if provided).
        fallback_cols: Broader group columns (e.g. ['_index']) used when
                       the primary group has fewer than min_peers.

    Returns:
        Series of z-scores aligned with df's index.
    """
    def _z(s):
        valid = s.dropna()
        if len(valid) < min_peers:
            return pd.Series(np.nan, index=s.index)
        return (s - s.mean()) / s.std()

    result = df.groupby(group_cols, sort=False)[col].transform(_z)

    if fallback_cols is not None:
        mask = result.isna() & df[col].notna()
        if mask.any():
            fallback = df.groupby(fallback_cols, sort=False)[col].transform(_z)
            result = result.where(~mask, fallback)

    return result


def composite_score(*cols):
    """Row-wise mean of non-null score columns. Returns NaN if all inputs are NaN."""
    stacked = np.column_stack(cols)
    with np.errstate(invalid='ignore'):
        return np.nanmean(stacked, axis=1)


def dense_rank_desc(series):
    """Dense rank within a group, highest value = rank 1. NaN stays NaN."""
    return series.rank(method='dense', ascending=False, na_option='keep').astype('Int16')


def dense_rank_asc(series):
    """Dense rank within a group, lowest value = rank 1. NaN stays NaN."""
    return series.rank(method='dense', ascending=True, na_option='keep').astype('Int16')
