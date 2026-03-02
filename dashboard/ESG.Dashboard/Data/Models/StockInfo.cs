namespace ESG.Dashboard.Data.Models;

public class StockInfo
{
    public string Index { get; set; } = "";
    public string Symbol { get; set; } = "";
    public string? ShortName { get; set; }
    public string? Sector { get; set; }
    public string? Industry { get; set; }
    public string? Country { get; set; }
}
