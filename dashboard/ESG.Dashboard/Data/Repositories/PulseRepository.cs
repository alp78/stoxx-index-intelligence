using Dapper;
using ESG.Dashboard.Data.Models;

namespace ESG.Dashboard.Data.Repositories;

/// <summary>Reads from bronze.pulse and bronze.pulse_tickers — real-time price snapshots and activity rankings.</summary>
public class PulseRepository
{
    private readonly DbConnectionFactory _db;

    /// <summary>Initializes a new instance with the specified database connection factory.</summary>
    /// <param name="db">Factory for creating SQL Server connections.</param>
    public PulseRepository(DbConnectionFactory db) => _db = db;

    /// <summary>
    /// Current pulse snapshot for each active ticker across all indices.
    /// The pulse table contains only the latest snapshot per index (truncate-and-reload),
    /// so no ROW_NUMBER is needed. Returns up to ~30 rows (10 per index).
    /// </summary>
    public async Task<IEnumerable<PulseSnapshot>> GetLatestPulseAsync()
    {
        var sql = """
            SELECT t._index        AS [Index],
                   t.symbol         AS Symbol,
                   p.timestamp      AS Timestamp,
                   d.short_name     AS ShortName,
                   d.country        AS Country,
                   d.sector         AS Sector,
                   p.current_price  AS CurrentPrice,
                   p.open_price     AS OpenPrice,
                   p.day_high       AS DayHigh,
                   p.day_low        AS DayLow,
                   p.previous_close AS PreviousClose,
                   p.price_change   AS PriceChange,
                   p.price_change_pct AS PriceChangePct,
                   p.bid            AS Bid,
                   p.ask            AS Ask,
                   p.bid_size       AS BidSize,
                   p.ask_size       AS AskSize,
                   p.spread         AS Spread,
                   p.current_volume AS CurrentVolume,
                   p.average_volume_10day AS AverageVolume10Day,
                   p.volume_ratio   AS VolumeRatio
            FROM bronze.pulse_tickers t
            LEFT JOIN bronze.pulse p
                ON t._index = p._index AND t.symbol = p.symbol
            LEFT JOIN silver.index_dim d
                ON t._index = d._index AND t.symbol = d.symbol AND d.is_current = 1
            ORDER BY t._index, t.rank
            """;
        return await _db.WithDeadlockRetryAsync(conn =>
            conn.QueryAsync<PulseSnapshot>(sql));
    }

    /// <summary>
    /// Activity-ranked tickers from the latest discovery batch per index.
    /// </summary>
    public async Task<IEnumerable<PulseTicker>> GetActiveTickersAsync()
    {
        var sql = """
            WITH latest AS (
                SELECT _index, symbol, rank, discovered_at,
                       volume_surge, range_intensity, vol_z, rng_z, activity_score,
                       ROW_NUMBER() OVER (
                           PARTITION BY _index, symbol
                           ORDER BY discovered_at DESC
                       ) AS rn
                FROM bronze.pulse_tickers
            )
            SELECT l._index        AS [Index],
                   l.symbol         AS Symbol,
                   l.rank           AS Rank,
                   l.discovered_at  AS DiscoveredAt,
                   d.short_name     AS ShortName,
                   d.country        AS Country,
                   l.volume_surge   AS VolumeSurge,
                   l.range_intensity AS RangeIntensity,
                   l.vol_z          AS VolZ,
                   l.rng_z          AS RngZ,
                   l.activity_score AS ActivityScore
            FROM latest l
            LEFT JOIN silver.index_dim d
                ON l._index = d._index AND l.symbol = d.symbol AND d.is_current = 1
            WHERE l.rn = 1
            ORDER BY l._index, l.rank
            """;
        return await _db.WithDeadlockRetryAsync(conn =>
            conn.QueryAsync<PulseTicker>(sql));
    }

}
