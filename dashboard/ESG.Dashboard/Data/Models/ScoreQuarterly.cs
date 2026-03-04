namespace ESG.Dashboard.Data.Models;

/// <summary>Quarterly fundamental scores per stock — quality/moat, financial health flags, and governance risk.</summary>
public class ScoreQuarterly
{
    public string Index { get; set; } = "";
    public string Symbol { get; set; } = "";
    public DateTime AsOfDate { get; set; }
    public string? Sector { get; set; }

    // Quality / Moat
    public double? GrossMarginZscore { get; set; }
    public double? RoeZscore { get; set; }
    public double? OperatingMarginZscore { get; set; }
    public double? LeverageZscore { get; set; }
    public double? FcfYield { get; set; }
    public double? FcfYieldZscore { get; set; }
    public double? QualityScore { get; set; }
    public short? QualityRank { get; set; }

    // Financial Health Flags
    public bool? FlagLiquidity { get; set; }
    public bool? FlagLeverage { get; set; }
    public bool? FlagCashburn { get; set; }
    public bool? FlagDoublDecline { get; set; }
    public byte? HealthFlagsCount { get; set; }
    public string? HealthRiskLevel { get; set; }

    // Governance
    public double? OverallRisk { get; set; }
    public double? AuditRisk { get; set; }
    public double? BoardRisk { get; set; }
    public double? CompensationRisk { get; set; }
    public double? ShareholderRightsRisk { get; set; }
    public double? GovernanceScore { get; set; }
    public short? GovernanceRank { get; set; }
    public double? Beta { get; set; }
    public double? GovernanceVsQuality { get; set; }
}
