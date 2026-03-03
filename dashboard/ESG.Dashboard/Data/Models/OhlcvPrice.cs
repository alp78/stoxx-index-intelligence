namespace ESG.Dashboard.Data.Models;

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
