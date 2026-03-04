namespace ESG.Dashboard.Data.Models;

/// <summary>Daily signal scores per stock — value, momentum, sentiment composites plus price/weight data.</summary>
public class ScoreDaily
{
    public string Index { get; set; } = "";
    public string Symbol { get; set; } = "";
    public DateTime ScoreDate { get; set; }
    public string? Sector { get; set; }

    // Relative Value
    public double? PeZscore { get; set; }
    public double? PbZscore { get; set; }
    public double? EvEbitdaZscore { get; set; }
    public double? YieldZscore { get; set; }
    public double? RelativeValueScore { get; set; }
    public short? RelativeValueRank { get; set; }

    // Momentum
    public double? RelativeStrength { get; set; }
    public double? Sma50Ratio { get; set; }
    public double? Sma200Ratio { get; set; }
    public double? DistFrom52wHigh { get; set; }
    public double? MomentumScore { get; set; }
    public short? MomentumRank { get; set; }

    // Analyst Sentiment
    public double? ImpliedUpside { get; set; }
    public double? RecommendationMean { get; set; }
    public bool? PriceFallingAnalystsBullish { get; set; }
    public double? SentimentScore { get; set; }
    public short? SentimentRank { get; set; }

    // Composite
    public double? CompositeScore { get; set; }
    public short? CompositeRank { get; set; }

    // Moving Averages
    public double? Sma30Close { get; set; }
    public double? Sma90Close { get; set; }

    // Index Weight
    public long? MarketCap { get; set; }
    public double? IndexWeight { get; set; }

    // Company Info
    public string? ShortName { get; set; }
    public string? Country { get; set; }
    public string? Currency { get; set; }

    // Price & Performance
    public double? CurrentPrice { get; set; }
    public double? DayChangePct { get; set; }
    public double? FiveDayChangePct { get; set; }
    public double? YtdChangePct { get; set; }
}
