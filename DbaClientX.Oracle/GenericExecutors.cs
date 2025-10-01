using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DBAClientX.OracleGeneric;

/// <summary>
/// Generic, minimal static entry points for hosts to execute SQL via provider connection string pieces.
/// </summary>
public static class GenericExecutors
{
    // For Oracle, DatabaseClientBase API uses (host, serviceName, username, password)
    public static Task<int> ExecuteSqlAsync(string host, string serviceName, string username, string password, string sql, IDictionary<string, object?>? parameters = null, CancellationToken ct = default)
    {
        var cli = new DBAClientX.Oracle();
        return cli.ExecuteNonQueryAsync(host, serviceName, username, password, sql, parameters, cancellationToken: ct);
    }
}

