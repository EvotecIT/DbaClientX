using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

namespace DBAClientX.SqlServerGeneric;

/// <summary>
/// Generic, minimal static entry points for hosts to execute SQL via a connection string.
/// </summary>
public static class GenericExecutors
{
    public static Task<int> ExecuteSqlAsync(string connectionString, string sql, IDictionary<string, object?>? parameters = null, CancellationToken ct = default)
    {
        var b = new SqlConnectionStringBuilder(connectionString);
        var cli = new DBAClientX.SqlServer();
        return cli.ExecuteNonQueryAsync(b.DataSource, b.InitialCatalog, b.IntegratedSecurity, sql, parameters, cancellationToken: ct, username: b.UserID, password: b.Password);
    }
}
