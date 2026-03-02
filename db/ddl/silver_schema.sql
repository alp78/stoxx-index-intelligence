-- ============================================================================
-- Silver schema: Create silver layer
-- Silver = cleaned, deduplicated, gap-filled (SCD2 on dimensions only)
-- Fully idempotent: safe to run multiple times.
-- ============================================================================

USE stoxx;
GO

SET QUOTED_IDENTIFIER ON;
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver');
GO

-- ============================================================================
-- 1. Index Dimensions (SCD Type 2)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'silver' AND t.name = 'index_dim')
CREATE TABLE silver.index_dim (
    id                      INT IDENTITY(1,1) PRIMARY KEY,
    _index                  VARCHAR(20)     NOT NULL,
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
    price_data_start        DATE,

    valid_from              DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    valid_to                DATETIME2       NULL,
    is_current              BIT             NOT NULL DEFAULT 1
);
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'UX_silver_index_dim_current')
CREATE UNIQUE INDEX UX_silver_index_dim_current
    ON silver.index_dim (_index, symbol) WHERE is_current = 1;
GO

-- Drop old non-unique index if it exists (replaced by UX_silver_index_dim_current)
IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_silver_index_dim_current')
DROP INDEX IX_silver_index_dim_current ON silver.index_dim;
GO

-- ============================================================================
-- 2. Daily Signals (deduplicated, one row per symbol per day)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'silver' AND t.name = 'signals_daily')
CREATE TABLE silver.signals_daily (
    id                      INT IDENTITY(1,1) PRIMARY KEY,
    _index                  VARCHAR(20)     NOT NULL,
    symbol                  VARCHAR(20)     NOT NULL,
    signal_date             DATE            NOT NULL,

    current_price           FLOAT,
    forward_pe              FLOAT,
    price_to_book           FLOAT,
    ev_to_ebitda            FLOAT,
    dividend_yield          FLOAT,
    market_cap              BIGINT,
    beta                    FLOAT,
    fifty_two_week_change   FLOAT,
    sandp_52_week_change    FLOAT,
    fifty_day_average       FLOAT,
    two_hundred_day_average FLOAT,
    dist_from_52_week_high  FLOAT,
    target_median_price     FLOAT,
    recommendation_mean     FLOAT,
    upside_potential         FLOAT
);
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_silver_signals_daily_symbol_date')
CREATE UNIQUE INDEX IX_silver_signals_daily_symbol_date
    ON silver.signals_daily (_index, symbol, signal_date);
GO

-- ============================================================================
-- 3. Quarterly Signals (deduplicated, one row per symbol per quarter)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'silver' AND t.name = 'signals_quarterly')
CREATE TABLE silver.signals_quarterly (
    id                      INT IDENTITY(1,1) PRIMARY KEY,
    _index                  VARCHAR(20)     NOT NULL,
    symbol                  VARCHAR(20)     NOT NULL,
    as_of_date              DATE            NOT NULL,

    gross_margins           FLOAT,
    operating_margins       FLOAT,
    return_on_equity        FLOAT,
    revenue_growth          FLOAT,
    earnings_growth         FLOAT,
    shares_outstanding      BIGINT,
    float_shares            BIGINT,
    debt_to_equity          FLOAT,
    current_ratio           FLOAT,
    free_cashflow           BIGINT,
    last_fiscal_year_end    DATE,
    most_recent_quarter     DATE,
    overall_risk            INT,
    audit_risk              INT,
    board_risk              INT,
    compensation_risk       INT,
    shareholder_rights_risk INT,
    esg_populated           BIT
);
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_silver_signals_quarterly_symbol_date')
CREATE UNIQUE INDEX IX_silver_signals_quarterly_symbol_date
    ON silver.signals_quarterly (_index, symbol, as_of_date);
GO

-- ============================================================================
-- 4. Per-index OHLCV tables are created dynamically by setup_index.py
--    from ingestion/indices.json. Do NOT add them here.
-- ============================================================================

PRINT 'Silver layer created successfully.';
GO
