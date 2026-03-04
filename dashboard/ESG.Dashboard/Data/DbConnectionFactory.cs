using Microsoft.Data.SqlClient;
using System.Data;

namespace ESG.Dashboard.Data;

/// <summary>Creates IDbConnection instances from the configured SQL Server connection string.</summary>
public class DbConnectionFactory
{
    private readonly string _connectionString;

    public DbConnectionFactory(string connectionString)
    {
        _connectionString = connectionString;
    }

    public IDbConnection Create() => new SqlConnection(_connectionString);
}
