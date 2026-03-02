# Ingestion Pipeline

The pipeline fetches market data from yfinance, stages it as JSON files, then loads it into
SQL Server bronze tables. All SQL transforms (bronze → silver → gold) are covered in
[medallion.md](medallion.md).

## Data Flow

```
data/definitions/*.json          ← index configuration (symbol lists)
        │
        ▼
    Fetchers (yfinance API)
        │
        ▼
data/stage/*.json                ← daily OHLCV, signals
data/dimensions/*_dim.json       ← enriched stock identities
data/pulse/*_tickers.json        ← top 10 most active stocks
data/pulse/*_pulse.json          ← real-time quotes
        │
        ▼
    Loaders (JSON → SQL)
        │
        ▼
bronze.{prefix}_ohlcv            ← raw price history
bronze.index_dim                 ← stock metadata
bronze.signals_daily             ← PE, yield, momentum snapshot
bronze.signals_quarterly         ← margins, ROE, governance snapshot
bronze.pulse_tickers             ← active ticker rankings
bronze.pulse                     ← live quotes
```

## Pipeline Steps

The orchestrator (`utils/run_pipeline.py`) has 16 steps organized in fetch → load → transform
triplets. Only the fetch and load steps are covered here.

| Step | Name | Type | Description |
|------|------|------|-------------|
| 1 | Fetch OHLCV | fetch | Daily price history (smart gap detection) |
| 2 | Load OHLCV | load | Merge into bronze (insert missing only) |
| 4 | Fetch signals daily | fetch | PE, yield, momentum, sentiment from yfinance |
| 5 | Load signals daily | load | Truncate & reload bronze snapshot |
| 6 | Fetch signals quarterly | fetch | Margins, ROE, governance from yfinance |
| 7 | Load signals quarterly | load | Truncate & reload bronze snapshot |
| 10 | Fetch pulse tickers | fetch | Discover top 10 most active stocks per index |
| 11 | Load pulse tickers | load | Truncate & reload bronze |
| 12 | Fetch pulse | fetch | Real-time quotes for discovered tickers |
| 13 | Load pulse | load | Truncate & reload bronze |

Steps 3, 8-9, 14-16 are transforms (see [medallion.md](medallion.md)).

### Running the pipeline

```bash
python utils/run_pipeline.py              # All 16 steps
python utils/run_pipeline.py --step 1     # Single step
python utils/run_pipeline.py --from 4     # Steps 4-16
```

In production, Airflow triggers Cloud Run jobs with `--step N` arguments. Locally, you run
the same commands directly.

## Setting Up an Index

### 1. Create a definition file

Each index needs a JSON file in `data/definitions/`:

```json
{
    "name": "Euro STOXX 50",
    "symbols": ["SAP", "SIE.DE", "ALV.DE", "ASML.AS", "TTE.PA"],
    "history_start": "2021-01-01"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `name` | yes | Display name |
| `symbols` | yes | yfinance ticker symbols |
| `history_start` | no | Defaults to `2021-01-01`. Earliest date to fetch OHLCV |
| `file_prefix` | no | Derived from filename by removing `_` (e.g., `euro_stoxx_50` → `eurostoxx50`) |

The filename is the index key: `data/definitions/euro_stoxx_50.json` → key `euro_stoxx_50`.

Symbol format follows yfinance conventions:
- US stocks: `AAPL`, `MSFT` (no suffix)
- German: `SAP` or `SIE.DE` (exchange suffix)
- Dutch: `ASML.AS`
- French: `TTE.PA`
- Hong Kong: `0700.HK`

### 2. Run setup

```bash
python utils/setup_index.py euro_stoxx_50     # One index
python utils/setup_index.py                   # All indices
```

Setup is idempotent — safe to re-run. It performs the full sequence:

1. Creates bronze and silver OHLCV tables for the index
2. Fetches dimensions (company name, sector, country, exchange) from yfinance
3. Loads dimensions into `bronze.index_dim`
4. Refreshes the trading calendar for relevant exchanges
5. Runs SCD2 merge into `silver.index_dim`
6. Fetches full OHLCV history (from `history_start` to today)
7. Loads OHLCV into bronze, transforms to silver (gap-filled)
8. Fetches and loads daily + quarterly signals
9. Discovers pulse tickers and fetches pulse snapshots
10. Runs all gold transforms (scores, index performance)

### 3. Verify

```bash
python utils/run_pipeline.py --step 1     # Should fetch only today's gap
```

If step 1 fetches the full history again, the silver table wasn't populated correctly.

### Removing an index

```bash
python utils/drop_index.py euro_stoxx_50
```

Deletes all data from bronze, silver, and gold tables, drops the per-index OHLCV tables,
and removes all JSON files. The definition file is preserved for re-setup.

## Index Discovery

The config system (`utils/config.py`) auto-discovers indices by scanning `data/definitions/*.json`.
No registration step — drop a file in the directory and it becomes available.

Key helper functions:

| Function | Example | Output |
|----------|---------|--------|
| `data_path(key, "ohlcv")` | `data_path("euro_stoxx_50", "ohlcv")` | `data/stage/eurostoxx50_ohlcv.json` |
| `bronze_ohlcv(key)` | `bronze_ohlcv("euro_stoxx_50")` | `"bronze.eurostoxx50_ohlcv"` |
| `silver_ohlcv(key)` | `silver_ohlcv("euro_stoxx_50")` | `"silver.eurostoxx50_ohlcv"` |

The file prefix strips underscores from the key: `euro_stoxx_50` → `eurostoxx50`.

## JSON Staging Area

All fetched data lands as JSON files before being loaded into the database. This decouples
fetching (API calls, network) from loading (DB writes), so a failed load doesn't require
re-fetching.

```
data/
├── definitions/              # Index configs (committed to git)
│   ├── euro_stoxx_50.json
│   └── stoxx_asia_50.json
│
├── dimensions/               # Enriched stock identities
│   ├── eurostoxx50_dim.json
│   └── stoxxasia50_dim.json
│
├── stage/                    # Daily fetches
│   ├── eurostoxx50_ohlcv.json
│   ├── eurostoxx50_signals_daily.json
│   └── eurostoxx50_signals_quarterly.json
│
├── history/                  # OHLCV archive (initial full fetch)
│   └── eurostoxx50_ohlcv_history.json
│
└── pulse/                    # Real-time market data
    ├── eurostoxx50_tickers.json
    └── eurostoxx50_pulse.json
```

All writes use atomic file operations (write to temp file, then `os.replace`) to prevent
corrupt JSON from partial writes or crashes.

## Fetchers

All fetchers live in the `fetchers/` directory and call the yfinance API. They are
rate-limited (0.1-0.15s between requests) to avoid throttling.

### Dimensions (`fetch_index_dim.py`)

Enriches each symbol with company metadata from `yfinance.Ticker(symbol).info`:

```
Definition symbols → yfinance .info → dimensions JSON
```

Output per symbol:
- **Identity:** longName, shortName, sector, industry, country, city
- **Exchange:** exchange code, timezone, currency
- **Date range:** `_range_start` (from definition), `_price_data_start` (from yfinance)

Symbols with no yfinance data or a start date after `history_start` are skipped with a warning.

### OHLCV (`fetch_ohlcv.py`)

Fetches daily price history with smart gap detection:

```
Silver OHLCV (last date per symbol)
    + exchange timezone (from bronze.index_dim)
    → determine missing date range
    → yfinance .history(start, end, interval='1d')
    → stage JSON
```

**Gap detection logic per symbol:**
1. Query `silver.{prefix}_ohlcv` for the last date
2. Get exchange timezone from `bronze.index_dim` to determine "today" in the stock's local time
3. If last silver date = today → skip (already up-to-date)
4. If last silver date exists → fetch from that date (1-day overlap for safety)
5. If no silver data → fetch from `history_start` (full history)

Output fields: `symbol, date, open, high, low, close, adj_close, volume, dividends, stock_splits`

Prices are normalized to 4 decimal places.

### Signals Daily (`fetch_signals_daily.py`)

Snapshot of current valuation, momentum, and sentiment metrics per symbol:

```
Dimension registry → yfinance .info → signals_daily JSON
```

Metrics collected:
- **Price:** currentPrice, forwardPE, priceToBook, EV/EBITDA, dividendYield, beta
- **Momentum:** 50-day & 200-day moving averages, 52-week change, distance from 52-week high
- **Sentiment:** analyst target price, recommendation mean (1=strong buy, 5=sell), upside potential

This is a full snapshot — the entire file is replaced each run.

### Signals Quarterly (`fetch_signals_quarterly.py`)

Fundamental and governance data, keyed by the most recent fiscal quarter:

```
Dimension registry → yfinance .info → signals_quarterly JSON
```

Metrics collected:
- **Quality:** grossMargins, operatingMargins, returnOnEquity, revenueGrowth, earningsGrowth
- **Capital:** debtToEquity, currentRatio, freeCashflow, sharesOutstanding
- **Fiscal dates:** lastFiscalYearEnd, mostRecentQuarter (used as the time key)
- **Governance:** ISS risk scores — overall, audit, board, compensation, shareholder rights

Full snapshot, replaced each run.

### Pulse Tickers (`fetch_pulse.py → discover_pulse_tickers()`)

Discovers the top 10 most active stocks per index, run hourly:

```
All symbols → yfinance volume + day range → z-score ranking → top 10
```

Activity score formula:
```
volumeSurge   = current_volume / avg_10day_volume
rangeIntensity = (dayHigh - dayLow) / previousClose
activityScore = 0.5 × z(volumeSurge) + 0.5 × z(rangeIntensity)
```

Z-scores are computed within the index. The top 10 by `activityScore` are selected.

Output: symbol list + ranking details (volumeSurge, rangeIntensity, activityScore per stock).

### Pulse Snapshots (`fetch_pulse.py → fetch_pulse()`)

Lightweight quotes for the pre-discovered tickers, run every 5 minutes:

```
Discovered tickers → yfinance .info (subset) → pulse JSON
```

Data per ticker:
- **Price:** current, open, dayHigh, dayLow, previousClose, change, changePct
- **Order book:** bid, ask, bidSize, askSize, spread
- **Volume:** current, 10-day average, ratio

Full snapshot, replaced each run. This powers the dashboard's Live page.

## Loaders

All loaders live in the `loaders/` directory. They read JSON files and write to bronze tables.
Every loader is wrapped in a SQL transaction — on error, the entire batch rolls back.

### Load Strategies

| Strategy | Tables | Behavior |
|----------|--------|----------|
| **Merge** | OHLCV | Insert only rows where (symbol, date) doesn't exist in bronze |
| **Truncate & reload** | Everything else | Delete all rows for this index, insert fresh snapshot |

OHLCV uses merge because it's append-only historical data — you never want to lose old rows.
Everything else is a point-in-time snapshot where the latest data replaces the previous.

### OHLCV Loader (`load_ohlcv.py`)

```
stage JSON → query existing (symbol, date) pairs → insert missing rows → commit
```

1. Loads the staged JSON file
2. Queries bronze for all existing `(symbol, date)` pairs
3. Filters to only new rows
4. Inserts in batches of 10,000 rows per commit
5. Logs: records loaded, records skipped (duplicates)

### Signals Loaders (`load_signals_daily.py`, `load_signals_quarterly.py`)

```
stage JSON → DELETE WHERE _index = ? → INSERT all rows → commit
```

1. Loads the staged JSON file
2. Deletes all existing rows for the current index from the bronze table
3. Flattens the nested JSON structure (e.g., `price_metrics.forwardPE` → `forward_pe` column)
4. Inserts all rows
5. Single transaction — old data is only deleted if new data inserts succeed

### Dimension Loader (`load_index_dim.py`)

```
dimensions JSON → DELETE WHERE _index = ? → INSERT all rows → commit
```

Same truncate & reload pattern. Adds metadata columns: `_index` (index key) and
`_ingested_at` (current timestamp).

### Pulse Loaders (`load_pulse_tickers.py`, `load_pulse.py`)

```
pulse JSON → DELETE WHERE _index = ? → INSERT all rows → commit
```

Pulse tickers includes the `rank` column (1-10) and `activity_score`.
Pulse snapshots flatten the nested price/book/volume structure into columns.

## Preconditions & Error Handling

Each step checks prerequisites before running:
- Definition file must exist for the index
- JSON staging file must exist and contain valid JSON
- Required bronze/silver tables must exist

If a prerequisite is missing, the step **skips that index** with a log message suggesting
how to fix it (e.g., "run setup_index.py first"). Other indices continue normally.

Example skip log:
```
WARN | Skipping euro_stoxx_50: no staged OHLCV file | step=load_ohlcv hint=run step 1 first
```

## Environment

The pipeline reads database credentials from `.env` at the project root:

```
SQL_HOST=localhost        # or Cloud SQL private IP in production
SQL_PORT=1434             # 1434 for local Docker, 1433 for Cloud SQL
SQL_DATABASE=stoxx
SQL_USER=sa               # or sqlserver for Cloud SQL
SA_PASSWORD=<password>
```

### Local vs Production

| | Local | Production (Cloud Run) |
|---|---|---|
| Database | Docker SQL Server on localhost:1434 | Cloud SQL private IP:1433 |
| Credentials | `.env` file | Cloud Run env vars (set in Terraform) |
| JSON files | `data/` directory on disk | Ephemeral container filesystem |
| Trigger | `python utils/run_pipeline.py` | Airflow → `CloudRunExecuteJobOperator` |
| User | `sa` | `sqlserver` |

In production, the pipeline container image includes the `data/definitions/` files. Staged
JSON files are written to the container's ephemeral filesystem — they don't persist between
Cloud Run executions, which is fine because each execution fetches fresh data.

## Logging

All pipeline steps log to both console and `logs/pipeline.jsonl`:

```
INFO  | Fetched OHLCV | step=fetch_ohlcv index=euro_stoxx_50 symbols=50 rows=2340 duration_ms=18500
INFO  | Loaded OHLCV  | step=load_ohlcv  index=euro_stoxx_50 inserted=2340 skipped=0 duration_ms=450
WARN  | Skipping SIE.DE: already up-to-date | step=fetch_ohlcv index=euro_stoxx_50
```

The JSON log file uses Datadog-structured format for production monitoring.

## Adding a New Index

1. Create `data/definitions/my_new_index.json` with name, symbols, and optional history_start
2. Run `python utils/setup_index.py my_new_index`
3. Verify: `python utils/run_pipeline.py --step 1` should fetch only today's gap
4. The daily pipeline will include the new index automatically on next run

No code changes needed — the config system discovers definition files at runtime.
