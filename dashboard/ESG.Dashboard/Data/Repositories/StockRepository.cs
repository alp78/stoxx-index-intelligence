using Dapper;
using ESG.Dashboard.Data.Models;

namespace ESG.Dashboard.Data.Repositories;

public class StockRepository
{
    private readonly DbConnectionFactory _db;

    // Maps index keys to their silver OHLCV table names
    private static readonly Dictionary<string, string> OhlcvTables = new()
    {
        ["euro_stoxx_50"] = "silver.eurostoxx50_ohlcv",
        ["stoxx_usa_50"] = "silver.stoxxusa50_ohlcv",
        ["stoxx_asia_50"] = "silver.stoxxasia50_ohlcv",
    };

    public StockRepository(DbConnectionFactory db) => _db = db;

    public async Task<IEnumerable<StockInfo>> GetStocksAsync(string? index = null)
    {
        using var conn = _db.Create();
        var sql = """
            SELECT _index AS [Index], symbol AS Symbol,
                   short_name AS ShortName, sector AS Sector,
                   industry AS Industry, country AS Country
            FROM silver.index_dim
            WHERE is_current = 1
              AND (@Index IS NULL OR _index = @Index)
            ORDER BY _index, symbol
            """;
        return await conn.QueryAsync<StockInfo>(sql, new { Index = index });
    }

    public async Task<StockInfo?> GetStockAsync(string index, string symbol)
    {
        using var conn = _db.Create();
        var sql = """
            SELECT _index AS [Index], symbol AS Symbol,
                   short_name AS ShortName, sector AS Sector,
                   industry AS Industry, country AS Country
            FROM silver.index_dim
            WHERE is_current = 1 AND _index = @Index AND symbol = @Symbol
            """;
        return await conn.QueryFirstOrDefaultAsync<StockInfo>(sql, new { Index = index, Symbol = symbol });
    }

    public async Task<IEnumerable<OhlcvPrice>> GetOhlcvAsync(
        string index, string symbol, DateTime? from = null, DateTime? to = null)
    {
        if (!OhlcvTables.TryGetValue(index, out var table))
            return [];

        using var conn = _db.Create();
        // Table name is from a fixed dictionary, not user input
        // Compute MA 30/90 via SQL window functions (server-side, avoids client recalc)
        // CTE computes over full history; outer query filters by date range
        var sql = $"""
            WITH cte AS (
                SELECT symbol, date, [open], high, low, [close], adj_close, volume,
                       CASE WHEN [close] <> 0 THEN adj_close / [close] ELSE 1 END AS adj_ratio,
                       AVG(adj_close) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS sma_30,
                       AVG(adj_close) OVER (ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS sma_90,
                       COUNT(adj_close) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS cnt_30,
                       COUNT(adj_close) OVER (ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS cnt_90
                FROM {table}
                WHERE symbol = @Symbol AND adj_close IS NOT NULL
            )
            SELECT symbol AS Symbol, date AS Date,
                   [open] * adj_ratio AS [Open], high * adj_ratio AS High,
                   low * adj_ratio AS Low, adj_close AS [Close],
                   [close] AS RawClose, adj_close AS AdjClose, volume AS Volume,
                   CASE WHEN cnt_30 >= 30 THEN sma_30 END AS Sma30,
                   CASE WHEN cnt_90 >= 90 THEN sma_90 END AS Sma90
            FROM cte
            WHERE (@From IS NULL OR date >= @From)
              AND (@To IS NULL OR date <= @To)
            ORDER BY date
            """;
        return await conn.QueryAsync<OhlcvPrice>(sql, new { Symbol = symbol, From = from, To = to });
    }

    public async Task<IEnumerable<SymbolClose>> GetBatchClosesAsync(
        string index, string[] symbols, DateTime from)
    {
        if (!OhlcvTables.TryGetValue(index, out var table) || symbols.Length == 0)
            return [];

        using var conn = _db.Create();
        var sql = $"""
            SELECT symbol AS Symbol, date AS Date, adj_close AS [Close]
            FROM {table}
            WHERE symbol IN @Symbols AND date >= @From
              AND adj_close IS NOT NULL
            ORDER BY symbol, date
            """;
        return await conn.QueryAsync<SymbolClose>(sql, new { Symbols = symbols, From = from });
    }

    public async Task<IEnumerable<string>> GetIndexKeysAsync()
    {
        using var conn = _db.Create();
        var sql = "SELECT DISTINCT _index FROM silver.index_dim WHERE is_current = 1 ORDER BY _index";
        return await conn.QueryAsync<string>(sql);
    }
}
