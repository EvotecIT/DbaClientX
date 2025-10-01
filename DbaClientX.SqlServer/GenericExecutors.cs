using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

namespace DBAClientX.SqlServerGeneric;

/// <summary>
/// Generic, minimal static entry points for hosts to execute SQL or stored procedures via a connection string.
/// This is the reflection-friendly façade consumed by hosts (e.g., TestimoX.Service) to avoid hard provider dependencies.
/// Internally it forwards to the instance-based <see cref="DBAClientX.SqlServer"/> client.
/// </summary>
public static class GenericExecutors
{
    /// <summary>
    /// Executes a parameterized SQL statement using <paramref name="connectionString"/>.
    /// </summary>
    /// <param name="connectionString">A standard SQL Server connection string.</param>
    /// <param name="sql">SQL text to execute.</param>
    /// <param name="parameters">Parameter name/value map (e.g., {"@UserName":"alice"}).</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Number of affected rows (as reported by the provider).</returns>
    public static Task<int> ExecuteSqlAsync(string connectionString, string sql, IDictionary<string, object?>? parameters = null, CancellationToken ct = default)
    {
        var b = new SqlConnectionStringBuilder(connectionString);
        var cli = new DBAClientX.SqlServer();
        return cli.ExecuteNonQueryAsync(b.DataSource, b.InitialCatalog, b.IntegratedSecurity, sql, parameters, cancellationToken: ct, username: b.UserID, password: b.Password);
    }

    /// <summary>
    /// Executes a stored procedure using <paramref name="connectionString"/>.
    /// </summary>
    /// <param name="connectionString">A standard SQL Server connection string.</param>
    /// <param name="procedure">Stored procedure name (optionally schema-qualified).</param>
    /// <param name="parameters">Parameter name/value map.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Zero. This façade returns 0 to keep cross-provider signatures uniform.</returns>
    public static async Task<int> ExecuteProcedureAsync(string connectionString, string procedure, IDictionary<string, object?>? parameters = null, CancellationToken ct = default)
    {
        var b = new SqlConnectionStringBuilder(connectionString);
        var cli = new DBAClientX.SqlServer();
        await cli.ExecuteStoredProcedureAsync(b.DataSource, b.InitialCatalog, b.IntegratedSecurity, procedure, parameters, cancellationToken: ct, username: b.UserID, password: b.Password).ConfigureAwait(false);
        return 0;
    }
}
