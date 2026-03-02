-- ============================================================================
-- Silver schema: Create silver layer
-- Silver = cleaned, deduplicated, gap-filled (SCD2 on dimensions only)
-- ============================================================================

USE ESG;
GO

SET QUOTED_IDENTIFIER ON;
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver');
GO

-- ============================================================================
-- 1. Index Dimensions (SCD Type 2)
-- ============================================================================
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

CREATE INDEX IX_silver_index_dim_current
    ON silver.index_dim (_index, symbol) WHERE is_current = 1;
GO

-- ============================================================================
-- 2. Daily Signals (deduplicated, one row per symbol per day)
-- ============================================================================
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

CREATE UNIQUE INDEX IX_silver_signals_daily_symbol_date
    ON silver.signals_daily (_index, symbol, signal_date);
GO

-- ============================================================================
-- 3. Quarterly Signals (deduplicated, one row per symbol per quarter)
-- ============================================================================
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

CREATE UNIQUE INDEX IX_silver_signals_quarterly_symbol_date
    ON silver.signals_quarterly (_index, symbol, as_of_date);
GO

-- ============================================================================
-- 4. OHLCV - Euro Stoxx 50 (gap-filled from bronze + trading calendar)
-- ============================================================================
CREATE TABLE silver.eurostoxx50_ohlcv (
    id                      INT IDENTITY(1,1) PRIMARY KEY,
    symbol                  VARCHAR(20)     NOT NULL,
    date                    DATE            NOT NULL,
    [open]                  FLOAT,
    high                    FLOAT,
    low                     FLOAT,
    [close]                 FLOAT,
    adj_close               FLOAT,
    volume                  BIGINT,
    dividends               FLOAT,
    stock_splits            FLOAT,
    is_filled               BIT             NOT NULL DEFAULT 0     -- 1 = forward-filled, not actual data
);
GO

CREATE UNIQUE INDEX IX_silver_eurostoxx50_ohlcv_symbol_date
    ON silver.eurostoxx50_ohlcv (symbol, date);
GO

-- ============================================================================
-- 5. OHLCV - Stoxx USA 50 (gap-filled from bronze + trading calendar)
-- ============================================================================
CREATE TABLE silver.stoxxusa50_ohlcv (
    id                      INT IDENTITY(1,1) PRIMARY KEY,
    symbol                  VARCHAR(20)     NOT NULL,
    date                    DATE            NOT NULL,
    [open]                  FLOAT,
    high                    FLOAT,
    low                     FLOAT,
    [close]                 FLOAT,
    adj_close               FLOAT,
    volume                  BIGINT,
    dividends               FLOAT,
    stock_splits            FLOAT,
    is_filled               BIT             NOT NULL DEFAULT 0
);
GO

CREATE UNIQUE INDEX IX_silver_stoxxusa50_ohlcv_symbol_date
    ON silver.stoxxusa50_ohlcv (symbol, date);
GO

PRINT 'Silver layer created successfully.';
GO
