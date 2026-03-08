namespace ESG.Dashboard.Data.Models;

/// <summary>Lightweight symbol + date + adj_close row — used for batch momentum sparklines.</summary>
public class SymbolClose
{
    public string Symbol { get; set; } = "";
    public DateTime Date { get; set; }
    public double Close { get; set; }
    public long Volume { get; set; }
    public double High { get; set; }
    public double Low { get; set; }
}
