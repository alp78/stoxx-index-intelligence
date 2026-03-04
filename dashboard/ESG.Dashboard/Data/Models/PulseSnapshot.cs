namespace ESG.Dashboard.Data.Models;

/// <summary>Real-time price snapshot for a stock — price, book, volume data pushed via SignalR.</summary>
public class PulseSnapshot
{
    public string Index { get; set; } = "";
    public string Symbol { get; set; } = "";
    public DateTime Timestamp { get; set; }

    // Company info (from silver.index_dim)
    public string? ShortName { get; set; }
    public string? Country { get; set; }
    public string? Sector { get; set; }

    // Price
    public double? CurrentPrice { get; set; }
    public double? OpenPrice { get; set; }
    public double? DayHigh { get; set; }
    public double? DayLow { get; set; }
    public double? PreviousClose { get; set; }
    public double? PriceChange { get; set; }
    public double? PriceChangePct { get; set; }

    // Book
    public double? Bid { get; set; }
    public double? Ask { get; set; }
    public int? BidSize { get; set; }
    public int? AskSize { get; set; }
    public double? Spread { get; set; }

    // Volume
    public long? CurrentVolume { get; set; }
    public long? AverageVolume10Day { get; set; }
    public double? VolumeRatio { get; set; }
}
