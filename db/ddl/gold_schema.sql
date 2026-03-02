-- ============================================================================
-- Gold schema: Create gold layer
-- Gold = pre-computed analytics scores, index performance, dashboard-ready
-- Fully idempotent: safe to run multiple times.
-- ============================================================================

USE ESG;
GO

SET QUOTED_IDENTIFIER ON;
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold AUTHORIZATION dbo');
GO

-- Verify schema was created before proceeding
IF SCHEMA_ID('gold') IS NULL
BEGIN
    RAISERROR('ERROR: gold schema was not created. Check permissions (requires ALTER on database or CREATE SCHEMA).', 16, 1);
    RETURN;
END
GO

-- ============================================================================
-- 1. Daily Scores (relative value, momentum, analyst sentiment)
-- One row per (_index, symbol, score_date)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'gold' AND t.name = 'scores_daily')
CREATE TABLE gold.scores_daily (
    id                          INT IDENTITY(1,1) PRIMARY KEY,
    _index                      VARCHAR(20)     NOT NULL,
    symbol                      VARCHAR(20)     NOT NULL,
    score_date                  DATE            NOT NULL,
    sector                      NVARCHAR(100),

    -- Relative Value (z-scores within sector, inverted so cheap = positive)
    pe_zscore                   FLOAT,
    pb_zscore                   FLOAT,
    ev_ebitda_zscore            FLOAT,
    yield_zscore                FLOAT,
    relative_value_score        FLOAT,
    relative_value_rank         SMALLINT,

    -- Momentum
    relative_strength           FLOAT,
    sma_50_ratio                FLOAT,
    sma_200_ratio               FLOAT,
    dist_from_52w_high          FLOAT,
    momentum_score              FLOAT,
    momentum_rank               SMALLINT,

    -- Analyst Sentiment
    implied_upside              FLOAT,
    recommendation_mean         FLOAT,
    price_falling_analysts_bullish BIT,
    sentiment_score             FLOAT,
    sentiment_rank              SMALLINT,

    -- Composite
    composite_score             FLOAT,
    composite_rank              SMALLINT,

    _scored_at                  DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'UX_gold_scores_daily')
CREATE UNIQUE INDEX UX_gold_scores_daily
    ON gold.scores_daily (_index, symbol, score_date);
GO

-- Add MA columns (idempotent)
IF COL_LENGTH('gold.scores_daily', 'sma_30_close') IS NULL
    ALTER TABLE gold.scores_daily ADD sma_30_close FLOAT;
GO
IF COL_LENGTH('gold.scores_daily', 'sma_90_close') IS NULL
    ALTER TABLE gold.scores_daily ADD sma_90_close FLOAT;
GO
IF COL_LENGTH('gold.scores_daily', 'market_cap') IS NULL
    ALTER TABLE gold.scores_daily ADD market_cap BIGINT;
GO
IF COL_LENGTH('gold.scores_daily', 'index_weight') IS NULL
    ALTER TABLE gold.scores_daily ADD index_weight FLOAT;
GO
IF COL_LENGTH('gold.scores_daily', 'short_name') IS NULL
    ALTER TABLE gold.scores_daily ADD short_name NVARCHAR(200);
GO
IF COL_LENGTH('gold.scores_daily', 'country') IS NULL
    ALTER TABLE gold.scores_daily ADD country NVARCHAR(100);
GO
IF COL_LENGTH('gold.scores_daily', 'current_price') IS NULL
    ALTER TABLE gold.scores_daily ADD current_price FLOAT;
GO
IF COL_LENGTH('gold.scores_daily', 'day_change_pct') IS NULL
    ALTER TABLE gold.scores_daily ADD day_change_pct FLOAT;
GO
IF COL_LENGTH('gold.scores_daily', 'five_day_change_pct') IS NULL
    ALTER TABLE gold.scores_daily ADD five_day_change_pct FLOAT;
GO
IF COL_LENGTH('gold.scores_daily', 'ytd_change_pct') IS NULL
    ALTER TABLE gold.scores_daily ADD ytd_change_pct FLOAT;
GO
IF COL_LENGTH('gold.scores_daily', 'currency') IS NULL
    ALTER TABLE gold.scores_daily ADD currency VARCHAR(10);
GO

-- ============================================================================
-- 2. Quarterly Scores (quality/moat, financial health, governance)
-- One row per (_index, symbol, as_of_date)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'gold' AND t.name = 'scores_quarterly')
CREATE TABLE gold.scores_quarterly (
    id                          INT IDENTITY(1,1) PRIMARY KEY,
    _index                      VARCHAR(20)     NOT NULL,
    symbol                      VARCHAR(20)     NOT NULL,
    as_of_date                  DATE            NOT NULL,
    sector                      NVARCHAR(100),

    -- Quality / Moat (z-scores within sector)
    gross_margin_zscore         FLOAT,
    roe_zscore                  FLOAT,
    operating_margin_zscore     FLOAT,
    leverage_zscore             FLOAT,
    fcf_yield                   FLOAT,
    fcf_yield_zscore            FLOAT,
    quality_score               FLOAT,
    quality_rank                SMALLINT,

    -- Financial Health Flags
    flag_liquidity              BIT,
    flag_leverage               BIT,
    flag_cashburn               BIT,
    flag_double_decline         BIT,
    health_flags_count          TINYINT,
    health_risk_level           VARCHAR(10),

    -- Governance
    overall_risk                FLOAT,
    audit_risk                  FLOAT,
    board_risk                  FLOAT,
    compensation_risk           FLOAT,
    shareholder_rights_risk     FLOAT,
    governance_score            FLOAT,
    governance_rank             SMALLINT,
    beta                        FLOAT,
    governance_vs_quality       FLOAT,

    _scored_at                  DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'UX_gold_scores_quarterly')
CREATE UNIQUE INDEX UX_gold_scores_quarterly
    ON gold.scores_quarterly (_index, symbol, as_of_date);
GO

-- ============================================================================
-- 3. Index Performance (daily time series for cross-index comparison)
-- One row per (_index, perf_date)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'gold' AND t.name = 'index_performance')
CREATE TABLE gold.index_performance (
    id                          INT IDENTITY(1,1) PRIMARY KEY,
    _index                      VARCHAR(20)     NOT NULL,
    perf_date                   DATE            NOT NULL,

    -- Returns (equal-weighted across all index stocks)
    daily_return                FLOAT,
    cumulative_factor           FLOAT,
    rolling_30d_return          FLOAT,
    rolling_90d_return          FLOAT,
    ytd_return                  FLOAT,
    rolling_30d_volatility      FLOAT,

    -- Cross-sectional aggregates
    stocks_count                SMALLINT,
    avg_pe                      FLOAT,
    avg_pb                      FLOAT,
    avg_dividend_yield          FLOAT,
    avg_market_cap              BIGINT,

    _computed_at                DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'UX_gold_index_performance')
CREATE UNIQUE INDEX UX_gold_index_performance
    ON gold.index_performance (_index, perf_date);
GO

PRINT 'Gold layer created successfully.';
GO
