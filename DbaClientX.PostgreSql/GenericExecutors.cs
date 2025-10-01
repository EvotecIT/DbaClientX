using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace DBAClientX.PostgreSqlGeneric;

/// <summary>
/// Generic, minimal static entry points for hosts to execute SQL via a connection string.
/// </summary>
public static class GenericExecutors
{
    public static Task<int> ExecuteSqlAsync(string connectionString, string sql, IDictionary<string, object?>? parameters = null, CancellationToken ct = default)
    {
        var b = new NpgsqlConnectionStringBuilder(connectionString);
        var cli = new DBAClientX.PostgreSql();
        return cli.ExecuteNonQueryAsync(b.Host, b.Database, b.Username, b.Password, sql, parameters, cancellationToken: ct);
    }
}
