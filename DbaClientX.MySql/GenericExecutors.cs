using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MySqlConnector;

namespace DBAClientX.MySqlGeneric;

/// <summary>
/// Generic, minimal static entry points for hosts to execute SQL via a connection string.
/// </summary>
public static class GenericExecutors
{
    public static Task<int> ExecuteSqlAsync(string connectionString, string sql, IDictionary<string, object?>? parameters = null, CancellationToken ct = default)
    {
        var b = new MySqlConnectionStringBuilder(connectionString);
        var cli = new DBAClientX.MySql();
        return cli.ExecuteNonQueryAsync(b.Server, b.Database, b.UserID, b.Password, sql, parameters, cancellationToken: ct);
    }
}

