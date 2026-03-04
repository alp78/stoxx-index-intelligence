using Dapper;
using ESG.Dashboard.Data.Models;

namespace ESG.Dashboard.Data.Repositories;

/// <summary>Reads from gold.scores_daily and gold.scores_quarterly — stock-level factor scores.</summary>
public class ScoresRepository
{
    private readonly DbConnectionFactory _db;

    public ScoresRepository(DbConnectionFactory db) => _db = db;

    /// <summary>Latest daily scores for all stocks (per-index MAX date). Used by Overview and Stock Explorer.</summary>
    public async Task<IEnumerable<ScoreDaily>> GetDailyScoresAsync(string? index = null)
    {
        using var conn = _db.Create();
        var sql = """
            SELECT _index AS [Index], symbol AS Symbol, score_date AS ScoreDate, sector AS Sector,
                   pe_zscore AS PeZscore, pb_zscore AS PbZscore,
                   ev_ebitda_zscore AS EvEbitdaZscore, yield_zscore AS YieldZscore,
                   relative_value_score AS RelativeValueScore, relative_value_rank AS RelativeValueRank,
                   relative_strength AS RelativeStrength, sma_50_ratio AS Sma50Ratio,
                   sma_200_ratio AS Sma200Ratio, dist_from_52w_high AS DistFrom52wHigh,
                   momentum_score AS MomentumScore, momentum_rank AS MomentumRank,
                   implied_upside AS ImpliedUpside, recommendation_mean AS RecommendationMean,
                   price_falling_analysts_bullish AS PriceFallingAnalystsBullish,
                   sentiment_score AS SentimentScore, sentiment_rank AS SentimentRank,
                   composite_score AS CompositeScore, composite_rank AS CompositeRank,
                   sma_30_close AS Sma30Close, sma_90_close AS Sma90Close,
                   market_cap AS MarketCap, index_weight AS IndexWeight,
                   short_name AS ShortName, country AS Country, currency AS Currency,
                   current_price AS CurrentPrice, day_change_pct AS DayChangePct,
                   five_day_change_pct AS FiveDayChangePct, ytd_change_pct AS YtdChangePct
            FROM gold.scores_daily
            WHERE score_date = (SELECT MAX(s2.score_date) FROM gold.scores_daily s2 WHERE s2._index = gold.scores_daily._index)
              AND (@Index IS NULL OR _index = @Index)
            ORDER BY _index, index_weight DESC
            """;
        return await conn.QueryAsync<ScoreDaily>(sql, new { Index = index });
    }

    /// <summary>Single stock's latest daily scores (for the detail panel).</summary>
    public async Task<ScoreDaily?> GetDailyScoreAsync(string index, string symbol)
    {
        using var conn = _db.Create();
        var sql = """
            SELECT TOP 1
                   _index AS [Index], symbol AS Symbol, score_date AS ScoreDate, sector AS Sector,
                   pe_zscore AS PeZscore, pb_zscore AS PbZscore,
                   ev_ebitda_zscore AS EvEbitdaZscore, yield_zscore AS YieldZscore,
                   relative_value_score AS RelativeValueScore, relative_value_rank AS RelativeValueRank,
                   relative_strength AS RelativeStrength, sma_50_ratio AS Sma50Ratio,
                   sma_200_ratio AS Sma200Ratio, dist_from_52w_high AS DistFrom52wHigh,
                   momentum_score AS MomentumScore, momentum_rank AS MomentumRank,
                   implied_upside AS ImpliedUpside, recommendation_mean AS RecommendationMean,
                   price_falling_analysts_bullish AS PriceFallingAnalystsBullish,
                   sentiment_score AS SentimentScore, sentiment_rank AS SentimentRank,
                   composite_score AS CompositeScore, composite_rank AS CompositeRank,
                   sma_30_close AS Sma30Close, sma_90_close AS Sma90Close,
                   market_cap AS MarketCap, index_weight AS IndexWeight,
                   short_name AS ShortName, country AS Country, currency AS Currency,
                   current_price AS CurrentPrice, day_change_pct AS DayChangePct,
                   five_day_change_pct AS FiveDayChangePct, ytd_change_pct AS YtdChangePct
            FROM gold.scores_daily
            WHERE _index = @Index AND symbol = @Symbol
            ORDER BY score_date DESC
            """;
        return await conn.QueryFirstOrDefaultAsync<ScoreDaily>(sql, new { Index = index, Symbol = symbol });
    }

    /// <summary>Latest quarterly scores per stock (quality, governance, health flags).</summary>
    public async Task<IEnumerable<ScoreQuarterly>> GetQuarterlyScoresAsync(string? index = null)
    {
        using var conn = _db.Create();
        var sql = """
            WITH latest AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY _index, symbol ORDER BY as_of_date DESC) AS rn
                FROM gold.scores_quarterly
            )
            SELECT _index AS [Index], symbol AS Symbol, as_of_date AS AsOfDate, sector AS Sector,
                   gross_margin_zscore AS GrossMarginZscore, roe_zscore AS RoeZscore,
                   operating_margin_zscore AS OperatingMarginZscore,
                   leverage_zscore AS LeverageZscore,
                   fcf_yield AS FcfYield, fcf_yield_zscore AS FcfYieldZscore,
                   quality_score AS QualityScore, quality_rank AS QualityRank,
                   flag_liquidity AS FlagLiquidity, flag_leverage AS FlagLeverage,
                   flag_cashburn AS FlagCashburn, flag_double_decline AS FlagDoublDecline,
                   health_flags_count AS HealthFlagsCount, health_risk_level AS HealthRiskLevel,
                   overall_risk AS OverallRisk, audit_risk AS AuditRisk,
                   board_risk AS BoardRisk, compensation_risk AS CompensationRisk,
                   shareholder_rights_risk AS ShareholderRightsRisk,
                   governance_score AS GovernanceScore, governance_rank AS GovernanceRank,
                   beta AS Beta, governance_vs_quality AS GovernanceVsQuality
            FROM latest
            WHERE rn = 1
              AND (@Index IS NULL OR _index = @Index)
            ORDER BY _index, quality_rank
            """;
        return await conn.QueryAsync<ScoreQuarterly>(sql, new { Index = index });
    }

    /// <summary>Single stock's latest quarterly scores (for the detail panel).</summary>
    public async Task<ScoreQuarterly?> GetQuarterlyScoreAsync(string index, string symbol)
    {
        using var conn = _db.Create();
        var sql = """
            SELECT TOP 1
                   _index AS [Index], symbol AS Symbol, as_of_date AS AsOfDate, sector AS Sector,
                   gross_margin_zscore AS GrossMarginZscore, roe_zscore AS RoeZscore,
                   operating_margin_zscore AS OperatingMarginZscore,
                   leverage_zscore AS LeverageZscore,
                   fcf_yield AS FcfYield, fcf_yield_zscore AS FcfYieldZscore,
                   quality_score AS QualityScore, quality_rank AS QualityRank,
                   flag_liquidity AS FlagLiquidity, flag_leverage AS FlagLeverage,
                   flag_cashburn AS FlagCashburn, flag_double_decline AS FlagDoublDecline,
                   health_flags_count AS HealthFlagsCount, health_risk_level AS HealthRiskLevel,
                   overall_risk AS OverallRisk, audit_risk AS AuditRisk,
                   board_risk AS BoardRisk, compensation_risk AS CompensationRisk,
                   shareholder_rights_risk AS ShareholderRightsRisk,
                   governance_score AS GovernanceScore, governance_rank AS GovernanceRank,
                   beta AS Beta, governance_vs_quality AS GovernanceVsQuality
            FROM gold.scores_quarterly
            WHERE _index = @Index AND symbol = @Symbol
            ORDER BY as_of_date DESC
            """;
        return await conn.QueryFirstOrDefaultAsync<ScoreQuarterly>(sql, new { Index = index, Symbol = symbol });
    }

    /// <summary>Sector-level averages of all factor scores, joining daily + quarterly.</summary>
    public async Task<IEnumerable<SectorAggregate>> GetSectorAggregatesAsync(string? index = null)
    {
        using var conn = _db.Create();
        var sql = """
            WITH latest_q AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY _index, symbol ORDER BY as_of_date DESC) AS rn
                FROM gold.scores_quarterly
            )
            SELECT d._index AS [Index], d.sector AS Sector,
                   COUNT(*) AS StockCount,
                   AVG(sd.relative_value_score) AS AvgRelativeValueScore,
                   AVG(sd.momentum_score) AS AvgMomentumScore,
                   AVG(sd.sentiment_score) AS AvgSentimentScore,
                   AVG(sd.composite_score) AS AvgCompositeScore,
                   AVG(sq.quality_score) AS AvgQualityScore,
                   AVG(sq.governance_score) AS AvgGovernanceScore,
                   SUM(CAST(ISNULL(sq.health_flags_count, 0) AS INT)) AS HealthFlagsTotal
            FROM gold.scores_daily sd
            JOIN latest_q sq
                ON sd._index = sq._index AND sd.symbol = sq.symbol AND sq.rn = 1
            JOIN silver.index_dim d
                ON sd._index = d._index AND sd.symbol = d.symbol AND d.is_current = 1
            WHERE sd.score_date = (SELECT MAX(s2.score_date) FROM gold.scores_daily s2 WHERE s2._index = sd._index)
              AND (@Index IS NULL OR d._index = @Index)
            GROUP BY d._index, d.sector
            ORDER BY d._index, AVG(sd.composite_score) DESC
            """;
        return await conn.QueryAsync<SectorAggregate>(sql, new { Index = index });
    }
}
