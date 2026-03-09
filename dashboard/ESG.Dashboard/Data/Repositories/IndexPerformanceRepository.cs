using Dapper;
using ESG.Dashboard.Data.Models;

namespace ESG.Dashboard.Data.Repositories;

/// <summary>Reads from gold.index_performance — daily index-level metrics.</summary>
public class IndexPerformanceRepository
{
    private readonly DbConnectionFactory _db;

    public IndexPerformanceRepository(DbConnectionFactory db) => _db = db;

    /// <summary>Full time-series of index performance, optionally filtered by index and date range.</summary>
    public async Task<IEnumerable<IndexPerformance>> GetPerformanceAsync(
        string? index = null, DateTime? from = null, DateTime? to = null)
    {
        var sql = """
            SELECT _index AS [Index], perf_date AS PerfDate,
                   daily_return AS DailyReturn, cumulative_factor AS CumulativeFactor,
                   rolling_30d_return AS Rolling30dReturn, rolling_90d_return AS Rolling90dReturn,
                   ytd_return AS YtdReturn, rolling_30d_volatility AS Rolling30dVolatility,
                   stocks_count AS StocksCount,
                   avg_pe AS AvgPe, avg_pb AS AvgPb,
                   avg_dividend_yield AS AvgDividendYield, avg_market_cap AS AvgMarketCap
            FROM gold.index_performance
            WHERE (@Index IS NULL OR _index = @Index)
              AND (@From IS NULL OR perf_date >= @From)
              AND (@To IS NULL OR perf_date <= @To)
            ORDER BY _index, perf_date
            """;
        return await _db.WithDeadlockRetryAsync(conn =>
            conn.QueryAsync<IndexPerformance>(sql, new { Index = index, From = from, To = to }));
    }

    /// <summary>Most recent performance row per index (for the snapshot strip).</summary>
    public async Task<IEnumerable<IndexPerformance>> GetLatestSnapshotAsync()
    {
        var sql = """
            SELECT p._index AS [Index], p.perf_date AS PerfDate,
                   p.daily_return AS DailyReturn, p.cumulative_factor AS CumulativeFactor,
                   p.rolling_30d_return AS Rolling30dReturn, p.rolling_90d_return AS Rolling90dReturn,
                   p.ytd_return AS YtdReturn, p.rolling_30d_volatility AS Rolling30dVolatility,
                   p.stocks_count AS StocksCount,
                   p.avg_pe AS AvgPe, p.avg_pb AS AvgPb,
                   p.avg_dividend_yield AS AvgDividendYield, p.avg_market_cap AS AvgMarketCap
            FROM gold.index_performance p
            INNER JOIN (
                SELECT _index, MAX(perf_date) AS max_date
                FROM gold.index_performance
                GROUP BY _index
            ) latest ON p._index = latest._index AND p.perf_date = latest.max_date
            ORDER BY p._index
            """;
        return await _db.WithDeadlockRetryAsync(conn =>
            conn.QueryAsync<IndexPerformance>(sql));
    }
}
