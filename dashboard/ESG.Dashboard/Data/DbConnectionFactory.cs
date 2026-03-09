using Microsoft.Data.SqlClient;
using System.Data;

namespace ESG.Dashboard.Data;

/// <summary>Creates IDbConnection instances from the configured SQL Server connection string.</summary>
public class DbConnectionFactory
{
    private const int DeadlockErrorNumber = 1205;
    private const int MaxRetries = 3;

    private readonly string _connectionString;

    public DbConnectionFactory(string connectionString)
    {
        _connectionString = connectionString;
    }

    public IDbConnection Create() => new SqlConnection(_connectionString);

    /// <summary>Executes a database operation with automatic retry on deadlock (error 1205).</summary>
    public async Task<T> WithDeadlockRetryAsync<T>(Func<IDbConnection, Task<T>> operation)
    {
        for (int attempt = 1; attempt <= MaxRetries; attempt++)
        {
            try
            {
                using var conn = Create();
                return await operation(conn);
            }
            catch (SqlException ex) when (ex.Number == DeadlockErrorNumber && attempt < MaxRetries)
            {
                await Task.Delay(attempt * 100);
            }
        }
        // Final attempt — let it throw
        using var finalConn = Create();
        return await operation(finalConn);
    }
}
