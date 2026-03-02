namespace ESG.Dashboard.Data.Models;

public class SectorAggregate
{
    public string Index { get; set; } = "";
    public string Sector { get; set; } = "";
    public int StockCount { get; set; }
    public double? AvgRelativeValueScore { get; set; }
    public double? AvgMomentumScore { get; set; }
    public double? AvgSentimentScore { get; set; }
    public double? AvgCompositeScore { get; set; }
    public double? AvgQualityScore { get; set; }
    public double? AvgGovernanceScore { get; set; }
    public int HealthFlagsTotal { get; set; }
}
