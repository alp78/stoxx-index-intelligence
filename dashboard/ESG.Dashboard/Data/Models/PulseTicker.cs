namespace ESG.Dashboard.Data.Models;

/// <summary>Top activity ticker — stocks ranked by volume surge and range intensity for the pulse feed.</summary>
public class PulseTicker
{
    public string Index { get; set; } = "";
    public string Symbol { get; set; } = "";
    public int Rank { get; set; }
    public DateTime DiscoveredAt { get; set; }

    // Company info (from silver.index_dim)
    public string? ShortName { get; set; }
    public string? Country { get; set; }

    // Activity metrics
    public double? VolumeSurge { get; set; }
    public double? RangeIntensity { get; set; }
    public double? VolZ { get; set; }
    public double? RngZ { get; set; }
    public double? ActivityScore { get; set; }
}
