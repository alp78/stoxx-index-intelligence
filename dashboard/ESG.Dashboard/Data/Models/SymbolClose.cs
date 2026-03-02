namespace ESG.Dashboard.Data.Models;

public class SymbolClose
{
    public string Symbol { get; set; } = "";
    public DateTime Date { get; set; }
    public double Close { get; set; }
}
