using Dapper;

namespace ESG.Dashboard.Data;

/// <summary>
/// Loads index metadata (display name, OHLCV table, color, currency) from bronze.dim_index
/// and dominant exchange info (timezone, market hours) from silver.index_dim.
/// Singleton — loaded once at startup, used by all pages to avoid hardcoded switch expressions.
/// </summary>
public class IndexRegistry
{
    private readonly DbConnectionFactory _db;
    private Dictionary<string, IndexMeta>? _indices;
    private Dictionary<string, ExchangeMeta>? _exchanges;
    private readonly SemaphoreSlim _lock = new(1, 1);

    private static readonly string[] DefaultColors =
        ["#4285F4", "#66BB6A", "#EF5350", "#FFA726", "#AB47BC", "#26C6DA", "#EC407A", "#8D6E63"];

    /// <summary>Initializes a new instance with the specified database connection factory.</summary>
    /// <param name="db">Factory for creating SQL Server connections.</param>
    public IndexRegistry(DbConnectionFactory db) => _db = db;

    /// <summary>Loads index metadata and dominant exchange info from the database if not already cached. Thread-safe.</summary>
    public async Task EnsureLoadedAsync()
    {
        if (_indices is not null) return;

        await _lock.WaitAsync();
        try
        {
            if (_indices is not null) return;

            using var conn = _db.Create();
            try
            {
                var rows = await conn.QueryAsync<(string index_key, string display_name, string file_prefix, string? color, string? currency)>(
                    "SELECT index_key, display_name, file_prefix, color, currency FROM bronze.dim_index");

                _indices = new Dictionary<string, IndexMeta>(StringComparer.OrdinalIgnoreCase);
                int i = 0;
                foreach (var (key, name, prefix, color, currency) in rows)
                {
                    _indices[key] = new IndexMeta
                    {
                        Key = key,
                        DisplayName = name,
                        OhlcvTable = $"silver.{prefix}_ohlcv",
                        Color = !string.IsNullOrEmpty(color) ? color : DefaultColors[i % DefaultColors.Length],
                        Currency = currency ?? ""
                    };
                    i++;
                }

                // Load dominant exchange per index (most common timezone)
                var exchRows = await conn.QueryAsync<(string _index, string exchange, string tz_name, string tz_short, int cnt)>(
                    @"SELECT _index, full_exchange_name, exchange_timezone_name, exchange_timezone_short, COUNT(*) as cnt
                      FROM silver.index_dim WHERE is_current = 1
                      GROUP BY _index, full_exchange_name, exchange_timezone_name, exchange_timezone_short
                      ORDER BY _index, cnt DESC");

                _exchanges = new Dictionary<string, ExchangeMeta>(StringComparer.OrdinalIgnoreCase);
                foreach (var row in exchRows)
                {
                    // First (highest count) wins — dominant exchange for this index
                    if (!_exchanges.ContainsKey(row._index))
                    {
                        _exchanges[row._index] = new ExchangeMeta
                        {
                            ExchangeName = row.exchange,
                            TimeZoneId = row.tz_name,
                            TimeZoneShort = row.tz_short
                        };
                    }
                }
            }
            catch
            {
                _indices = new Dictionary<string, IndexMeta>(StringComparer.OrdinalIgnoreCase);
                _exchanges = new Dictionary<string, ExchangeMeta>(StringComparer.OrdinalIgnoreCase);
            }
        }
        finally { _lock.Release(); }
    }

    /// <summary>Returns the human-readable display name for the given index key (e.g. "Euro STOXX 50").</summary>
    public string GetDisplayName(string key) =>
        _indices is not null && _indices.TryGetValue(key, out var m) ? m.DisplayName : FormatKeyFallback(key);

    /// <summary>Returns the silver-layer OHLCV table name for the given index key (e.g. "silver.eurostoxx50_ohlcv").</summary>
    public string? GetOhlcvTable(string key) =>
        _indices is not null && _indices.TryGetValue(key, out var m)
            ? m.OhlcvTable
            : $"silver.{key.Replace("_", "")}_ohlcv";

    /// <summary>Returns the chart color hex code assigned to the given index key.</summary>
    public string GetColor(string key) =>
        _indices is not null && _indices.TryGetValue(key, out var m) ? m.Color : "#A0AEC0";

    /// <summary>Returns the base currency for the given index key (e.g. "EUR", "USD").</summary>
    public string GetCurrency(string key) =>
        _indices is not null && _indices.TryGetValue(key, out var m) ? m.Currency : "";

    /// <summary>Returns the dominant exchange name for the index (e.g. "NYSE", "XETRA").</summary>
    public string GetExchangeName(string key) =>
        _exchanges is not null && _exchanges.TryGetValue(key, out var e) ? e.ExchangeName : "--";

    /// <summary>Returns the IANA timezone ID for the dominant exchange (e.g. "America/New_York").</summary>
    public string GetTimeZoneId(string key) =>
        _exchanges is not null && _exchanges.TryGetValue(key, out var e) ? e.TimeZoneId : "UTC";

    /// <summary>Returns the short timezone label (e.g. "EST", "CET", "JST").</summary>
    public string GetTimeZoneShort(string key) =>
        _exchanges is not null && _exchanges.TryGetValue(key, out var e) ? e.TimeZoneShort : "UTC";

    /// <summary>Returns the current local time string at the dominant exchange.</summary>
    public string GetExchangeLocalTime(string key)
    {
        var tzId = GetTimeZoneId(key);
        try
        {
            var tz = TimeZoneInfo.FindSystemTimeZoneById(tzId);
            var local = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, tz);
            return $"{local:HH:mm} {GetTimeZoneShort(key)}";
        }
        catch { return "--"; }
    }

    /// <summary>Returns whether the dominant exchange is approximately open right now.</summary>
    public bool IsMarketOpen(string key)
    {
        var tzId = GetTimeZoneId(key);
        try
        {
            var tz = TimeZoneInfo.FindSystemTimeZoneById(tzId);
            var local = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, tz);
            if (local.DayOfWeek is DayOfWeek.Saturday or DayOfWeek.Sunday) return false;
            var t = local.TimeOfDay;
            return t >= new TimeSpan(9, 0, 0) && t <= new TimeSpan(17, 30, 0);
        }
        catch { return false; }
    }

    /// <summary>Converts an index key like "euro_stoxx_50" into a title-cased fallback display name.</summary>
    private static string FormatKeyFallback(string key) =>
        string.Join(' ', key.Split('_').Select(w =>
            w.Length > 0 ? char.ToUpper(w[0]) + w[1..] : w));

    /// <summary>Internal model representing index metadata loaded from bronze.dim_index.</summary>
    private class IndexMeta
    {
        public required string Key { get; init; }
        public required string DisplayName { get; init; }
        public required string OhlcvTable { get; init; }
        public required string Color { get; init; }
        public required string Currency { get; init; }
    }

    /// <summary>Internal model representing dominant exchange info for an index.</summary>
    private class ExchangeMeta
    {
        public required string ExchangeName { get; init; }
        public required string TimeZoneId { get; init; }
        public required string TimeZoneShort { get; init; }
    }
}
