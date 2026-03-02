-- ============================================================================
-- Bronze schema: Create stoxx database and bronze layer
-- Bronze = raw data, 1:1 with source JSON
-- Fully idempotent: safe to run multiple times.
-- ============================================================================

-- Create database
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'stoxx')
    CREATE DATABASE stoxx;
GO

USE stoxx;
GO

-- Create bronze schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');
GO

-- ============================================================================
-- 1. Index Dimensions (source: data/stage/*_dim.json, refreshed yearly)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'bronze' AND t.name = 'index_dim')
CREATE TABLE bronze.index_dim (
    id                      INT IDENTITY(1,1) PRIMARY KEY,
    _index                  VARCHAR(20)     NOT NULL,
    _ingested_at            DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),

    symbol                  VARCHAR(20)     NOT NULL,
    long_name               NVARCHAR(200),
    short_name              NVARCHAR(100),
    sector                  NVARCHAR(100),
    sector_key              VARCHAR(100),
    industry                NVARCHAR(200),
    industry_key            VARCHAR(200),
    country                 NVARCHAR(100),
    city                    NVARCHAR(100),
    website                 VARCHAR(500),
    long_business_summary   NVARCHAR(MAX),
    exchange                VARCHAR(20),
    full_exchange_name      NVARCHAR(100),
    exchange_timezone_name  VARCHAR(50),
    exchange_timezone_short VARCHAR(10),
    currency                VARCHAR(10),
    financial_currency      VARCHAR(10),
    quote_type              VARCHAR(20),
    market                  VARCHAR(50),
    range_start             DATE,
    price_data_start        DATE
);
GO

IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_bronze_index_dim_index')
DROP INDEX IX_bronze_index_dim_index ON bronze.index_dim;
GO

CREATE INDEX IX_bronze_index_dim_index
    ON bronze.index_dim (_index, symbol);
GO

-- ============================================================================
-- 2. Per-index OHLCV tables are created dynamically by setup_index.py
--    from ingestion/indices.json. Do NOT add them here.
-- ============================================================================

-- ============================================================================
-- 3. Daily Signals (source: data/stage/*_signals_daily.json, refreshed daily)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'bronze' AND t.name = 'signals_daily')
CREATE TABLE bronze.signals_daily (
    id                      INT IDENTITY(1,1) PRIMARY KEY,
    _index                  VARCHAR(20)     NOT NULL,
    _ingested_at            DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),

    symbol                  VARCHAR(20)     NOT NULL,
    timestamp               DATETIME2       NOT NULL,

    -- price_metrics
    current_price           FLOAT,
    forward_pe              FLOAT,
    price_to_book           FLOAT,
    ev_to_ebitda            FLOAT,
    dividend_yield          FLOAT,
    market_cap              BIGINT,
    beta                    FLOAT,

    -- market_context
    fifty_two_week_change   FLOAT,
    sandp_52_week_change    FLOAT,

    -- momentum_signals
    fifty_day_average       FLOAT,
    two_hundred_day_average FLOAT,
    dist_from_52_week_high  FLOAT,

    -- sentiment_signals
    target_median_price     FLOAT,
    recommendation_mean     FLOAT,
    upside_potential         FLOAT
);
GO

-- Replace old (symbol, timestamp) index with (_index, symbol, timestamp)
-- All operations filter by _index first (truncate-per-index, per-index reads)
IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_bronze_signals_daily_symbol_ts')
DROP INDEX IX_bronze_signals_daily_symbol_ts ON bronze.signals_daily;
GO

IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_bronze_signals_daily_index_symbol')
DROP INDEX IX_bronze_signals_daily_index_symbol ON bronze.signals_daily;
GO

CREATE INDEX IX_bronze_signals_daily_index_symbol
    ON bronze.signals_daily (_index, symbol, timestamp);
GO

-- ============================================================================
-- 5. Quarterly Signals (source: data/stage/*_signals_quarterly.json, refreshed quarterly)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'bronze' AND t.name = 'signals_quarterly')
CREATE TABLE bronze.signals_quarterly (
    id                      INT IDENTITY(1,1) PRIMARY KEY,
    _index                  VARCHAR(20)     NOT NULL,
    _ingested_at            DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),

    symbol                  VARCHAR(20)     NOT NULL,
    as_of_date              DATE            NOT NULL,

    -- quality_metrics
    gross_margins           FLOAT,
    operating_margins       FLOAT,
    return_on_equity        FLOAT,
    revenue_growth          FLOAT,
    earnings_growth         FLOAT,

    -- capital_structure
    shares_outstanding      BIGINT,
    float_shares            BIGINT,
    debt_to_equity          FLOAT,
    current_ratio           FLOAT,
    free_cashflow           BIGINT,

    -- fiscal_calendar
    last_fiscal_year_end    DATE,
    most_recent_quarter     DATE,

    -- governance
    overall_risk            INT,
    audit_risk              INT,
    board_risk              INT,
    compensation_risk       INT,
    shareholder_rights_risk INT,
    esg_populated           BIT
);
GO

-- Replace old (symbol, as_of_date) index with (_index, symbol, as_of_date)
IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_bronze_signals_quarterly_symbol_date')
DROP INDEX IX_bronze_signals_quarterly_symbol_date ON bronze.signals_quarterly;
GO

IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_bronze_signals_quarterly_index_symbol')
DROP INDEX IX_bronze_signals_quarterly_index_symbol ON bronze.signals_quarterly;
GO

CREATE INDEX IX_bronze_signals_quarterly_index_symbol
    ON bronze.signals_quarterly (_index, symbol, as_of_date);
GO

-- ============================================================================
-- 6. Pulse (source: data/pulse/*_pulse.json, refreshed every minute)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'bronze' AND t.name = 'pulse')
CREATE TABLE bronze.pulse (
    id                      BIGINT IDENTITY(1,1) PRIMARY KEY,
    _index                  VARCHAR(20)     NOT NULL,
    _ingested_at            DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),

    symbol                  VARCHAR(20)     NOT NULL,
    timestamp               DATETIME2       NOT NULL,

    -- price
    current_price           FLOAT,
    open_price              FLOAT,
    day_high                FLOAT,
    day_low                 FLOAT,
    previous_close          FLOAT,
    price_change            FLOAT,
    price_change_pct        FLOAT,

    -- book
    bid                     FLOAT,
    ask                     FLOAT,
    bid_size                INT,
    ask_size                INT,
    spread                  FLOAT,

    -- volume
    current_volume          BIGINT,
    average_volume_10day    BIGINT,
    volume_ratio            FLOAT
);
GO

-- Replace old (symbol, timestamp) index with (_index, symbol, timestamp)
-- Covers per-index merge lookup AND trim_pulse DELETE
IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_bronze_pulse_symbol_ts')
DROP INDEX IX_bronze_pulse_symbol_ts ON bronze.pulse;
GO

IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_bronze_pulse_index_symbol')
DROP INDEX IX_bronze_pulse_index_symbol ON bronze.pulse;
GO

CREATE INDEX IX_bronze_pulse_index_symbol
    ON bronze.pulse (_index, symbol, timestamp);
GO

-- ============================================================================
-- 7. Pulse Tickers (source: data/pulse/*_tickers.json, refreshed hourly)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'bronze' AND t.name = 'pulse_tickers')
CREATE TABLE bronze.pulse_tickers (
    id                      INT IDENTITY(1,1) PRIMARY KEY,
    _index                  VARCHAR(20)     NOT NULL,
    _ingested_at            DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),

    discovered_at           DATETIME2       NOT NULL,
    symbol                  VARCHAR(20)     NOT NULL,
    rank                    INT             NOT NULL,
    volume_surge            FLOAT,
    range_intensity         FLOAT,
    vol_z                   FLOAT,
    rng_z                   FLOAT,
    activity_score          FLOAT
);
GO

-- Replace old (discovered_at, _index) with (_index, discovered_at)
-- Main access pattern is DELETE WHERE _index = ? (truncate-per-index)
IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_bronze_pulse_tickers_discovered')
DROP INDEX IX_bronze_pulse_tickers_discovered ON bronze.pulse_tickers;
GO

IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_bronze_pulse_tickers_index')
DROP INDEX IX_bronze_pulse_tickers_index ON bronze.pulse_tickers;
GO

CREATE INDEX IX_bronze_pulse_tickers_index
    ON bronze.pulse_tickers (_index, discovered_at);
GO

-- ============================================================================
-- 8. Trading Calendar (source: exchange_calendars library, populated on seed)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'bronze' AND t.name = 'trading_calendar')
CREATE TABLE bronze.trading_calendar (
    date                    DATE            NOT NULL,
    exchange_code           VARCHAR(10)     NOT NULL,
    xc_code                 VARCHAR(10)     NOT NULL,
    year                    SMALLINT        NOT NULL,
    quarter                 TINYINT         NOT NULL,
    month                   TINYINT         NOT NULL,
    week_of_year            TINYINT         NOT NULL,
    day_of_week             TINYINT         NOT NULL,
    is_trading_day          BIT             NOT NULL,
    is_month_end            BIT             NOT NULL,
    is_quarter_end          BIT             NOT NULL,

    CONSTRAINT PK_trading_calendar PRIMARY KEY (date, exchange_code)
);
GO

-- Secondary index for calendar lookups: all queries filter by exchange_code first,
-- then is_trading_day, then scan/seek by date. PK (date, exchange_code) is wrong order.
IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_bronze_trading_calendar_exchange')
DROP INDEX IX_bronze_trading_calendar_exchange ON bronze.trading_calendar;
GO

CREATE INDEX IX_bronze_trading_calendar_exchange
    ON bronze.trading_calendar (exchange_code, is_trading_day, date);
GO

PRINT 'Bronze layer created successfully.';
GO
