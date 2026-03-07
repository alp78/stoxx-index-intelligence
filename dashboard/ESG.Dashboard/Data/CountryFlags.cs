using Dapper;

namespace ESG.Dashboard.Data;

/// <summary>
/// Resolves country names to ISO 3166-1 alpha-2 codes for flag-icons CSS.
/// Loaded once from bronze.dim_country at startup and cached in memory.
/// </summary>
public class CountryFlags
{
    private readonly DbConnectionFactory _db;
    private Dictionary<string, string>? _codes;
    private readonly SemaphoreSlim _lock = new(1, 1);

    public CountryFlags(DbConnectionFactory db) => _db = db;

    public async Task EnsureLoadedAsync()
    {
        if (_codes is not null) return;
        await _lock.WaitAsync();
        try
        {
            if (_codes is not null) return;
            using var conn = _db.Create();
            try
            {
                var rows = await conn.QueryAsync<(string country_name, string iso_alpha2)>(
                    "SELECT country_name, iso_alpha2 FROM bronze.dim_country");
                _codes = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                foreach (var (name, code) in rows)
                    _codes[name] = code.ToLowerInvariant();
                Console.WriteLine($"[CountryFlags] Loaded {_codes.Count} country codes");
                if (_codes.Count > 0)
                {
                    var sample = _codes.First();
                    Console.WriteLine($"[CountryFlags] Sample: '{sample.Key}' -> '{sample.Value}'");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[CountryFlags] Failed to load dim_country: {ex}");
                _codes = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>Get lowercase ISO alpha-2 code for a country name. Returns "" if not found.</summary>
    public string GetCode(string? country)
    {
        if (country is null || _codes is null || !_codes.TryGetValue(country, out var code))
            return "";
        return code;
    }
}
