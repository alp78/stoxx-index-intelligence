using Dapper;
using ESG.Dashboard.Data.Models;

namespace ESG.Dashboard.Data.Repositories;

/// <summary>Reads from silver.index_dim and silver.*_ohlcv — stock metadata and OHLCV prices.</summary>
public class StockRepository
{
    private readonly DbConnectionFactory _db;
    private readonly IndexRegistry _registry;

    /// <summary>Initializes a new instance with the specified database connection factory and index registry.</summary>
    /// <param name="db">Factory for creating SQL Server connections.</param>
    /// <param name="registry">Registry providing index-specific table names.</param>
    public StockRepository(DbConnectionFactory db, IndexRegistry registry)
    {
        _db = db;
        _registry = registry;
    }

    /// <summary>All current index constituents from silver.index_dim.</summary>
    public async Task<IEnumerable<StockInfo>> GetStocksAsync(string? index = null)
    {
        var sql = """
            SELECT _index AS [Index], symbol AS Symbol,
                   short_name AS ShortName, sector AS Sector,
                   industry AS Industry, country AS Country,
                   city AS City, long_business_summary AS LongBusinessSummary
            FROM silver.index_dim
            WHERE is_current = 1
              AND (@Index IS NULL OR _index = @Index)
            ORDER BY _index, symbol
            """;
        return await _db.WithDeadlockRetryAsync(conn =>
            conn.QueryAsync<StockInfo>(sql, new { Index = index }));
    }

    /// <summary>Single stock's metadata (name, sector, country).</summary>
    public async Task<StockInfo?> GetStockAsync(string index, string symbol)
    {
        var sql = """
            SELECT _index AS [Index], symbol AS Symbol,
                   short_name AS ShortName, sector AS Sector,
                   industry AS Industry, country AS Country,
                   city AS City, long_business_summary AS LongBusinessSummary
            FROM silver.index_dim
            WHERE is_current = 1 AND _index = @Index AND symbol = @Symbol
            """;
        return await _db.WithDeadlockRetryAsync(conn =>
            conn.QueryFirstOrDefaultAsync<StockInfo>(sql, new { Index = index, Symbol = symbol }));
    }

    /// <summary>OHLCV prices with server-side SMA 30/90 computation via SQL window functions.</summary>
    public async Task<IEnumerable<OhlcvPrice>> GetOhlcvAsync(
        string index, string symbol, DateTime? from = null, DateTime? to = null)
    {
        var table = _registry.GetOhlcvTable(index);
        if (table is null) return [];

        // Table name is from IndexRegistry (loaded from DB), not user input
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
        return await _db.WithDeadlockRetryAsync(conn =>
            conn.QueryAsync<OhlcvPrice>(sql, new { Symbol = symbol, From = from, To = to }));
    }

    /// <summary>Batch adj_close for multiple symbols — used for momentum sparkline charts.</summary>
    public async Task<IEnumerable<SymbolClose>> GetBatchClosesAsync(
        string index, string[] symbols, DateTime from)
    {
        var table = _registry.GetOhlcvTable(index);
        if (table is null || symbols.Length == 0)
            return [];

        var sql = $"""
            SELECT symbol AS Symbol, date AS Date, adj_close AS [Close], volume AS Volume,
                   CASE WHEN [close] <> 0 THEN high * adj_close / [close] ELSE high END AS High,
                   CASE WHEN [close] <> 0 THEN low * adj_close / [close] ELSE low END AS Low
            FROM {table}
            WHERE symbol IN @Symbols AND date >= @From
              AND adj_close IS NOT NULL
            ORDER BY symbol, date
            """;
        return await _db.WithDeadlockRetryAsync(conn =>
            conn.QueryAsync<SymbolClose>(sql, new { Symbols = symbols, From = from }));
    }

    /// <summary>Distinct index keys from silver.index_dim (used to populate dropdowns).</summary>
    public async Task<IEnumerable<string>> GetIndexKeysAsync()
    {
        var sql = "SELECT DISTINCT _index FROM silver.index_dim WHERE is_current = 1 ORDER BY _index";
        return await _db.WithDeadlockRetryAsync(conn =>
            conn.QueryAsync<string>(sql));
    }
}
