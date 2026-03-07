using Dapper;

namespace ESG.Dashboard.Data;

/// <summary>
/// Loads index metadata (display name, OHLCV table, color, currency) from bronze.dim_index.
/// Singleton — loaded once at startup, used by all pages to avoid hardcoded switch expressions.
/// </summary>
public class IndexRegistry
{
    private readonly DbConnectionFactory _db;
    private Dictionary<string, IndexMeta>? _indices;
    private readonly SemaphoreSlim _lock = new(1, 1);

    private static readonly string[] DefaultColors =
        ["#4285F4", "#66BB6A", "#EF5350", "#FFA726", "#AB47BC", "#26C6DA", "#EC407A", "#8D6E63"];

    public IndexRegistry(DbConnectionFactory db) => _db = db;

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
            }
            catch
            {
                _indices = new Dictionary<string, IndexMeta>(StringComparer.OrdinalIgnoreCase);
            }
        }
        finally { _lock.Release(); }
    }

    public string GetDisplayName(string key) =>
        _indices is not null && _indices.TryGetValue(key, out var m) ? m.DisplayName : FormatKeyFallback(key);

    public string? GetOhlcvTable(string key) =>
        _indices is not null && _indices.TryGetValue(key, out var m)
            ? m.OhlcvTable
            : $"silver.{key.Replace("_", "")}_ohlcv";

    public string GetColor(string key) =>
        _indices is not null && _indices.TryGetValue(key, out var m) ? m.Color : "#A0AEC0";

    public string GetCurrency(string key) =>
        _indices is not null && _indices.TryGetValue(key, out var m) ? m.Currency : "";

    private static string FormatKeyFallback(string key) =>
        string.Join(' ', key.Split('_').Select(w =>
            w.Length > 0 ? char.ToUpper(w[0]) + w[1..] : w));

    private class IndexMeta
    {
        public required string Key { get; init; }
        public required string DisplayName { get; init; }
        public required string OhlcvTable { get; init; }
        public required string Color { get; init; }
        public required string Currency { get; init; }
    }
}
