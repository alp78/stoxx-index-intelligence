namespace ESG.Dashboard.Data.Models;

/// <summary>Adjusted OHLCV price row with server-computed SMA 30/90 from silver.*_ohlcv tables.</summary>
public class OhlcvPrice
{
    public string Symbol { get; set; } = "";
    public DateTime Date { get; set; }
    public double? Open { get; set; }
    public double? High { get; set; }
    public double? Low { get; set; }
    public double? Close { get; set; }
    public double? RawClose { get; set; }
    public double? AdjClose { get; set; }
    public long? Volume { get; set; }
    public double? Sma30 { get; set; }
    public double? Sma90 { get; set; }
}
