namespace ESG.Dashboard.Data.Models;

public class IndexPerformance
{
    public string Index { get; set; } = "";
    public DateTime PerfDate { get; set; }
    public double? DailyReturn { get; set; }
    public double? CumulativeFactor { get; set; }
    public double? Rolling30dReturn { get; set; }
    public double? Rolling90dReturn { get; set; }
    public double? YtdReturn { get; set; }
    public double? Rolling30dVolatility { get; set; }
    public short? StocksCount { get; set; }
    public double? AvgPe { get; set; }
    public double? AvgPb { get; set; }
    public double? AvgDividendYield { get; set; }
    public long? AvgMarketCap { get; set; }
}
