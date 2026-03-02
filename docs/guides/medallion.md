# Medallion Architecture

The database uses a three-layer medallion architecture on SQL Server. Each layer
has its own schema (`bronze`, `silver`, `gold`) within the `stoxx` database.

```
Bronze (raw)                Silver (clean)              Gold (analytics)
────────────                ──────────────              ────────────────
bronze.{prefix}_ohlcv  ──►  silver.{prefix}_ohlcv      gold.scores_daily
bronze.index_dim       ──►  silver.index_dim            gold.scores_quarterly
bronze.signals_daily   ──►  silver.signals_daily        gold.index_performance
bronze.signals_quarterly ►  silver.signals_quarterly
bronze.pulse                     │
bronze.pulse_tickers             │
bronze.trading_calendar          │
                                 └──────────────────►  Gold transforms read
                                                       from silver + bronze
```

## Layer Principles

| Layer | Purpose | Retention | Write Pattern |
|-------|---------|-----------|---------------|
| **Bronze** | Raw data, 1:1 with source JSON | Latest snapshot (OHLCV trimmed to latest day) | Loaders write, transforms read |
| **Silver** | Cleaned, deduplicated, gap-filled | Full history | Transforms write (append/upsert) |
| **Gold** | Pre-computed scores, dashboard-ready | Full history (incremental) | Transforms write (replace date or append) |

## DDL Scripts

All schemas are defined in `db/ddl/` and are fully idempotent (`IF NOT EXISTS`):

```bash
python db/run_ddl.py           # Runs all 3 scripts in order
```

| Script | Creates |
|--------|---------|
| `bronze_schema.sql` | bronze schema + 6 shared tables + trading calendar |
| `silver_schema.sql` | silver schema + 3 shared tables |
| `gold_schema.sql` | gold schema + 3 analytics tables |

Per-index OHLCV tables (`bronze.{prefix}_ohlcv`, `silver.{prefix}_ohlcv`) are created
dynamically by `setup_index.py`, not in the DDL scripts.

---

## Bronze Layer

Bronze stores raw data exactly as loaded from JSON, plus metadata columns (`_index`,
`_ingested_at`). No transformations, no deduplication.

### Tables

#### `bronze.index_dim`

Stock identity and exchange metadata. One row per symbol per index.

| Column | Type | Description |
|--------|------|-------------|
| `_index` | VARCHAR(20) | Index key (e.g., `euro_stoxx_50`) |
| `symbol` | VARCHAR(20) | Ticker symbol |
| `long_name` | NVARCHAR(200) | Full company name |
| `sector` | NVARCHAR(100) | GICS sector |
| `industry` | NVARCHAR(200) | GICS industry |
| `country` | NVARCHAR(100) | Country of domicile |
| `exchange` | VARCHAR(20) | yfinance exchange code (e.g., `GER`, `AMS`) |
| `exchange_timezone_name` | VARCHAR(50) | IANA timezone (e.g., `Europe/Berlin`) |
| `currency` | VARCHAR(10) | Trading currency |
| `range_start` | DATE | From definition file |
| `price_data_start` | DATE | Earliest available price data from yfinance |

**Write pattern:** Truncate & reload per index. Full snapshot replaced each run.

**Index:** `(_index, symbol)` — covers per-index lookups and the OHLCV transform's
exchange mapping query.

#### `bronze.{prefix}_ohlcv`

Daily price history per index. Table name is dynamic: `bronze.eurostoxx50_ohlcv`.

| Column | Type | Description |
|--------|------|-------------|
| `symbol` | VARCHAR(20) | Ticker symbol |
| `date` | DATE | Trading date |
| `open`, `high`, `low`, `close` | FLOAT | OHLC prices (4 decimal places) |
| `adj_close` | FLOAT | Split/dividend-adjusted close |
| `volume` | BIGINT | Shares traded |
| `dividends` | FLOAT | Dividend paid on this date |
| `stock_splits` | FLOAT | Split factor (e.g., 4.0 for 4:1 split) |

**Write pattern:** Merge — insert only rows where `(symbol, date)` doesn't already exist.
Never deletes historical data.

**After transform:** Bronze is trimmed to keep only the latest day per symbol. The full
history lives in silver.

#### `bronze.signals_daily`

Point-in-time valuation, momentum, and sentiment snapshot.

| Column | Type | Description |
|--------|------|-------------|
| `current_price` | FLOAT | Last traded price |
| `forward_pe` | FLOAT | Forward price/earnings |
| `price_to_book` | FLOAT | Price/book value |
| `ev_to_ebitda` | FLOAT | Enterprise value / EBITDA |
| `dividend_yield` | FLOAT | Annual dividend / price |
| `market_cap` | BIGINT | Market capitalization |
| `beta` | FLOAT | Beta vs benchmark |
| `fifty_two_week_change` | FLOAT | 52-week price return |
| `sandp_52_week_change` | FLOAT | S&P 500 52-week return (for relative strength) |
| `fifty_day_average` | FLOAT | 50-day SMA |
| `two_hundred_day_average` | FLOAT | 200-day SMA |
| `dist_from_52_week_high` | FLOAT | Distance from 52-week high (negative) |
| `target_median_price` | FLOAT | Analyst median target price |
| `recommendation_mean` | FLOAT | Analyst consensus (1=strong buy, 5=sell) |
| `upside_potential` | FLOAT | (target - current) / current |

**Write pattern:** Truncate & reload per index.

#### `bronze.signals_quarterly`

Fundamental and governance snapshot, keyed by fiscal quarter.

| Column | Type | Description |
|--------|------|-------------|
| `as_of_date` | DATE | Snapshot date |
| `gross_margins` | FLOAT | Gross profit / revenue |
| `operating_margins` | FLOAT | Operating income / revenue |
| `return_on_equity` | FLOAT | Net income / shareholders' equity |
| `revenue_growth` | FLOAT | YoY revenue growth |
| `earnings_growth` | FLOAT | YoY earnings growth |
| `debt_to_equity` | FLOAT | Total debt / equity (%) |
| `current_ratio` | FLOAT | Current assets / current liabilities |
| `free_cashflow` | BIGINT | Free cash flow |
| `most_recent_quarter` | DATE | Fiscal quarter end date (used as silver key) |
| `overall_risk` .. `shareholder_rights_risk` | INT | ISS governance risk scores (1-10) |

**Write pattern:** Truncate & reload per index.

#### `bronze.pulse` and `bronze.pulse_tickers`

Real-time market activity data. `pulse_tickers` holds the top 10 most active stocks per
index (discovered hourly). `pulse` holds lightweight quotes for those tickers (every 5 min).

**Write pattern:** Truncate & reload per index. These are consumed directly by the
dashboard's Live page — no silver transform.

#### `bronze.trading_calendar`

Exchange trading schedules from the `exchange_calendars` library.

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Calendar date |
| `exchange_code` | VARCHAR(10) | yfinance exchange code |
| `xc_code` | VARCHAR(10) | `exchange_calendars` code (e.g., `XFRA`) |
| `is_trading_day` | BIT | 1 if the exchange was open |
| `is_month_end` | BIT | Last trading day of the month |
| `is_quarter_end` | BIT | Last trading day of the quarter |

**PK:** `(date, exchange_code)`. Covers all calendar days (trading + non-trading) from
the earliest `price_data_start` to 5 years in the future.

Used by the OHLCV gap-fill transform to know which dates need a price row.

---

## Silver Layer

Silver stores cleaned, deduplicated, and gap-filled data. This is the authoritative
historical record. All gold transforms read from silver.

### Tables

#### `silver.index_dim` (SCD Type 2)

Tracks stock identity changes over time. When a company changes its name, sector, country,
or any of 20 tracked attributes, the old row is closed and a new row is inserted.

| Column | Type | Description |
|--------|------|-------------|
| _(same columns as bronze.index_dim)_ | | |
| `valid_from` | DATETIME2 | When this version became active |
| `valid_to` | DATETIME2 | When this version was superseded (NULL = current) |
| `is_current` | BIT | 1 for the active version |

**Unique index:** `(_index, symbol) WHERE is_current = 1` — enforces one active row per symbol.

**Transform logic** (`transform_index_dim.py`):

1. Load bronze snapshot (all symbols per index)
2. Load silver current rows (`is_current = 1`)
3. Compare 20 attribute columns between bronze and silver:
   - **New symbol** (in bronze, not in silver): INSERT with `is_current=1`, `valid_from=now`
   - **Changed attributes**: UPDATE old row (`valid_to=now`, `is_current=0`), INSERT new row
   - **Removed symbol** (in silver, not in bronze): UPDATE (`valid_to=now`, `is_current=0`)

Attribute comparison normalizes types (str, date, float) to avoid false positives from
driver type differences.

#### `silver.{prefix}_ohlcv` (Gap-Filled)

Complete daily price history with no missing trading days.

| Column | Type | Description |
|--------|------|-------------|
| _(same columns as bronze OHLCV)_ | | |
| `is_filled` | BIT | 0 = real data from yfinance, 1 = gap-filled |

**Unique index:** `(symbol, date)`

**Transform logic** (`transform_ohlcv.py`):

Phase 1 — Gap-fill:

1. Load symbol → exchange mapping from `bronze.index_dim`
2. Pre-load trading calendars per exchange from `bronze.trading_calendar`
3. Load existing silver `(symbol, date)` pairs to skip
4. For each symbol, iterate over the exchange's trading days:
   - If the date exists in bronze: INSERT to silver with `is_filled = 0`
   - If the date is missing (market was open but no data) and we have a previous price:
     forward-fill with `is_filled = 1` (open = high = low = close = last known close,
     volume = 0)
   - If already in silver: skip
5. Commit every 5,000 rows

Phase 2 — Bronze trim:

After silver is updated, delete all but the latest row per symbol from bronze:

```sql
DELETE b FROM bronze.{prefix}_ohlcv b
INNER JOIN (
    SELECT symbol, MAX(date) AS max_date
    FROM bronze.{prefix}_ohlcv
    GROUP BY symbol
    HAVING COUNT(*) > 1
) m ON b.symbol = m.symbol AND b.date < m.max_date
```

This keeps bronze lean — the full history is in silver.

#### `silver.signals_daily` (Upsert)

One row per symbol per day. Builds a historical time series from daily snapshots.

**Unique index:** `(_index, symbol, signal_date)`

**Transform logic** (`transform_signals_daily.py`):

1. Load all existing silver rows into a lookup dict keyed by `(_index, symbol, signal_date)`
2. Read the current bronze snapshot (one row per symbol)
3. For each bronze row:
   - **New date** (key not in silver): INSERT
   - **Values changed** (key exists but values differ): UPDATE
   - **Unchanged** (identical values): skip

The `timestamp` from bronze is cast to `DATE` as the `signal_date` in silver.

#### `silver.signals_quarterly` (Upsert)

One row per symbol per fiscal quarter. Same upsert pattern as daily signals.

**Unique index:** `(_index, symbol, as_of_date)`

**Transform logic** (`transform_signals_quarterly.py`):

Same as daily signals, but keyed by `most_recent_quarter` from yfinance (the actual fiscal
quarter end date), falling back to `as_of_date` if null. This ensures a new row is created
only when a company reports a new quarter, not on every run.

---

## Gold Layer

Gold stores pre-computed analytics that are directly consumed by the dashboard.
All gold transforms use pandas for computation and write results back to SQL.

### Tables

#### `gold.scores_daily`

Per-stock daily analytics: relative value, momentum, sentiment, and a composite score.

**Key:** `(_index, symbol, score_date)` — unique index.

**Write pattern:** DELETE all rows for the score date, then INSERT fresh scores. Idempotent.

**Transform logic** (`transform_scores_daily.py`):

##### 1. Relative Value (z-scores, cheap = positive)

Z-scores are computed within sector when the sector has 3+ peers, falling back to
index-level when the sector is too small.

| Metric | Source | Inversion |
|--------|--------|-----------|
| `pe_zscore` | `forward_pe` | Inverted (lower PE = cheaper = positive) |
| `pb_zscore` | `price_to_book` | Inverted |
| `ev_ebitda_zscore` | `ev_to_ebitda` | Inverted |
| `yield_zscore` | `dividend_yield` | Not inverted (higher yield = better) |

`relative_value_score` = mean of the 4 z-scores.
`relative_value_rank` = dense rank within index (1 = cheapest).

##### 2. Momentum

| Metric | Formula |
|--------|---------|
| `relative_strength` | 52-week return − S&P 52-week return |
| `sma_50_ratio` | current_price / 50-day SMA |
| `sma_200_ratio` | current_price / 200-day SMA |
| `dist_from_52w_high` | distance from 52-week high (inverted: closer to high = stronger) |

Each metric is z-scored within the index, then averaged.
`momentum_rank` = dense rank (1 = strongest momentum).

##### 3. Analyst Sentiment

| Metric | Formula |
|--------|---------|
| `implied_upside` | (target_median_price / current_price) − 1 |
| `recommendation_mean` | Analyst consensus (inverted: lower = more bullish) |
| `price_falling_analysts_bullish` | Flag: price down >10% AND recommendation < 2.5 |

`sentiment_score` = mean of z-scored upside + z-scored inverted recommendation.
`sentiment_rank` = dense rank (1 = most bullish).

##### 4. Composite

`composite_score` = mean of relative_value, momentum, and sentiment scores.
`composite_rank` = dense rank (1 = best overall).

##### 5. Technical (from Silver OHLCV)

Computed via SQL window functions on `silver.{prefix}_ohlcv`:

| Metric | Computation |
|--------|-------------|
| `sma_30_close` | AVG(close) OVER 30 preceding rows (requires 30 rows) |
| `sma_90_close` | AVG(close) OVER 90 preceding rows (requires 90 rows) |
| `day_change_pct` | (close − prev_close) / prev_close |
| `five_day_change_pct` | (close − close_5d_ago) / close_5d_ago |
| `ytd_change_pct` | (close − first_close_of_year) / first_close_of_year |

Only rows with `volume > 0` are included (skips gap-filled rows).

##### 6. Index Weight

`index_weight` = stock's market_cap / sum(market_cap) within the index.

##### Additional columns

`short_name`, `country`, `currency`, `current_price` are joined from `silver.index_dim`
and `silver.signals_daily` for dashboard convenience.

---

#### `gold.scores_quarterly`

Per-stock quarterly analytics: quality, financial health flags, and governance.

**Key:** `(_index, symbol, as_of_date)` — unique index.

**Write pattern:** DELETE all rows for the as_of_date(s), then INSERT fresh.

**Transform logic** (`transform_scores_quarterly.py`):

##### 1. Quality / Moat (z-scores, higher = better)

| Metric | Source | Inversion |
|--------|--------|-----------|
| `gross_margin_zscore` | `gross_margins` | Not inverted |
| `roe_zscore` | `return_on_equity` | Not inverted |
| `operating_margin_zscore` | `operating_margins` | Not inverted |
| `fcf_yield_zscore` | `free_cashflow / market_cap` | Not inverted |
| `leverage_zscore` | `debt_to_equity` | Inverted (lower debt = better) |

`quality_score` = mean of 5 z-scores.
`quality_rank` = dense rank (1 = highest quality).

##### 2. Financial Health Flags (rule-based thresholds)

| Flag | Condition | What it means |
|------|-----------|---------------|
| `flag_liquidity` | `current_ratio < 1` | Can't cover short-term liabilities |
| `flag_leverage` | `debt_to_equity > 200` | Heavily indebted |
| `flag_cashburn` | `free_cashflow < 0` | Burning cash |
| `flag_double_decline` | `earnings_growth < 0 AND revenue_growth < 0` | Both shrinking |

`health_flags_count` = sum of flags (0-4).
`health_risk_level`:

| Count | Level |
|-------|-------|
| 0 | `healthy` |
| 1 | `watch` |
| 2 | `warning` |
| 3-4 | `critical` |

##### 3. Governance

`governance_score` = 10 − mean(overall_risk, audit_risk, board_risk, compensation_risk,
shareholder_rights_risk). Higher = better governance.

`governance_rank` = dense rank (1 = best governance).

`governance_vs_quality` = governance_score − quality_score. Positive means governance
is stronger than fundamentals.

---

#### `gold.index_performance`

Daily time series per index: market-cap-weighted returns, rolling metrics, and
cross-sectional aggregates.

**Key:** `(_index, perf_date)` — unique index.

**Write pattern:** Incremental append — only inserts dates newer than the latest existing row.
Never overwrites historical rows.

**Transform logic** (`transform_index_performance.py`):

##### 1. Daily Return (cap-weighted)

For each trading date:

```
daily_return = Σ(stock_return × market_cap) / Σ(market_cap)
```

Market cap is joined from `silver.signals_daily` using `merge_asof` with a 7-day tolerance
(signals may not exist for every OHLCV date). Falls back to equal weights if no market cap
data exists.

##### 2. Cumulative Factor

```
cumulative_factor = Π(1 + daily_return)    (geometric compounding from day 1)
```

##### 3. Rolling Returns

| Metric | Window | Formula |
|--------|--------|---------|
| `rolling_30d_return` | 30 days | Π(1 + daily_return) − 1 over trailing 30 days |
| `rolling_90d_return` | 90 days | Same, trailing 90 days |
| `rolling_30d_volatility` | 30 days | std(daily_return) × √252 (annualized) |

Requires the full window (min_periods = window size). Returns NaN until enough data
accumulates.

##### 4. YTD Return

Cumulative return within each calendar year, reset on Jan 1:

```
ytd_return = Π(1 + daily_return) − 1    (from Jan 1 to date)
```

##### 5. Cross-Sectional Aggregates (cap-weighted)

| Metric | Source |
|--------|--------|
| `avg_pe` | Cap-weighted mean of `forward_pe` |
| `avg_pb` | Cap-weighted mean of `price_to_book` |
| `avg_dividend_yield` | Cap-weighted mean of `dividend_yield` |
| `avg_market_cap` | Mean market cap across index stocks |

These are forward-filled from `silver.signals_daily` and merged using `merge_asof` with
a 3-day tolerance.

---

## Z-Score Computation

All gold z-scores use the shared `zscore_by_group()` function:

```python
zscore_by_group(df, col, ['_index', 'sector'], min_peers=3, fallback_cols=['_index'])
```

1. Group by `(_index, sector)` and compute `(value - mean) / std` within each group
2. If a sector has fewer than 3 non-null values, fall back to index-level z-scores
3. If the index also has fewer than 3 values, return NaN

This prevents distorted z-scores in small sectors (e.g., a single Utilities stock
in an index would always get z-score = 0 without the fallback).

`composite_score()` computes the row-wise mean of multiple z-score columns, ignoring NaN.
This means a stock with data for 3 out of 4 sub-scores still gets a composite.

---

## Transform Pipeline Steps

| Step | Transform | Source → Target |
|------|-----------|-----------------|
| 3 | OHLCV gap-fill + bronze trim | `bronze.{prefix}_ohlcv` → `silver.{prefix}_ohlcv` |
| 8 | Signals daily upsert | `bronze.signals_daily` → `silver.signals_daily` |
| 9 | Signals quarterly upsert | `bronze.signals_quarterly` → `silver.signals_quarterly` |
| 14 | Daily scores | `silver.signals_daily` + `silver.index_dim` + `silver.{prefix}_ohlcv` → `gold.scores_daily` |
| 15 | Quarterly scores | `silver.signals_quarterly` + `silver.signals_daily` + `silver.index_dim` → `gold.scores_quarterly` |
| 16 | Index performance | `silver.{prefix}_ohlcv` + `silver.signals_daily` → `gold.index_performance` |

The trading calendar and SCD2 dimension transforms run during setup and are not numbered
pipeline steps. They're called from `setup_index.py` and invoked before the OHLCV transform.

---

## Indexes

Every table uses targeted indexes to support the primary access patterns:

| Table | Index | Covers |
|-------|-------|--------|
| `bronze.index_dim` | `(_index, symbol)` | Per-index lookups, exchange mapping |
| `bronze.signals_daily` | `(_index, symbol, timestamp)` | Per-index truncate & scan |
| `bronze.signals_quarterly` | `(_index, symbol, as_of_date)` | Per-index truncate & scan |
| `bronze.pulse` | `(_index, symbol, timestamp)` | Per-index truncate & merge |
| `bronze.pulse_tickers` | `(_index, discovered_at)` | Per-index truncate |
| `bronze.trading_calendar` | `(exchange_code, is_trading_day, date)` | Calendar lookups per exchange |
| `silver.index_dim` | **unique** `(_index, symbol) WHERE is_current=1` | SCD2 current row |
| `silver.signals_daily` | **unique** `(_index, symbol, signal_date)` | Upsert key |
| `silver.signals_quarterly` | **unique** `(_index, symbol, as_of_date)` | Upsert key |
| `gold.scores_daily` | **unique** `(_index, symbol, score_date)` | Dashboard queries |
| `gold.scores_quarterly` | **unique** `(_index, symbol, as_of_date)` | Dashboard queries |
| `gold.index_performance` | **unique** `(_index, perf_date)` | Time series queries |

Gold indexes are unique to enforce one score per stock per date and to support the
dashboard's queries efficiently.
