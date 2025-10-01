using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DBAClientX.OracleGeneric;

/// <summary>
/// Generic, reflection-friendly façade for executing SQL or stored procedures via Oracle connection info.
/// Internally forwards to <see cref="DBAClientX.Oracle"/>.
/// </summary>
public static class GenericExecutors
{
    // For Oracle, DatabaseClientBase API uses (host, serviceName, username, password)
    /// <summary>
    /// Executes a parameterized SQL statement.
    /// </summary>
    /// <param name="host">Oracle host name or address.</param>
    /// <param name="serviceName">Oracle service/SID.</param>
    /// <param name="username">User name.</param>
    /// <param name="password">Password.</param>
    /// <param name="sql">SQL text to execute.</param>
    /// <param name="parameters">Parameter name/value map.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Number of affected rows.</returns>
    public static Task<int> ExecuteSqlAsync(string host, string serviceName, string username, string password, string sql, IDictionary<string, object?>? parameters = null, CancellationToken ct = default)
    {
        var cli = new DBAClientX.Oracle();
        return cli.ExecuteNonQueryAsync(host, serviceName, username, password, sql, parameters, cancellationToken: ct);
    }

    /// <summary>
    /// Executes a parameterized SQL statement using a full Oracle connection string.
    /// Prefer this overload when available to avoid manual connection string parsing.
    /// </summary>
    public static Task<int> ExecuteSqlAsync(string connectionString, string sql, IDictionary<string, object?>? parameters = null, CancellationToken ct = default)
    {
        var b = new global::Oracle.ManagedDataAccess.Client.OracleConnectionStringBuilder(connectionString);
        // DataSource is typically host[:port]/service or TNS name; we pass as-is to builder inside provider class via host/service.
        // Best effort split: host/serviceName if pattern contains '/'; otherwise treat both as DataSource for compatibility.
        var dataSource = b.DataSource ?? string.Empty;
        string host = dataSource;
        string service = dataSource;
        var slash = dataSource.LastIndexOf('/');
        if (slash > 0)
        {
            host = dataSource.Substring(0, slash);
            service = dataSource.Substring(slash + 1);
        }
        var cli = new DBAClientX.Oracle();
        return cli.ExecuteNonQueryAsync(host, service, b.UserID, b.Password, sql, parameters, cancellationToken: ct);
    }

    // Procedure variant mirrors the provider signature
    /// <summary>
    /// Executes a stored procedure.
    /// </summary>
    /// <param name="host">Oracle host name or address.</param>
    /// <param name="serviceName">Oracle service/SID.</param>
    /// <param name="username">User name.</param>
    /// <param name="password">Password.</param>
    /// <param name="procedure">Stored procedure name.</param>
    /// <param name="parameters">Parameter name/value map.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Zero. This façade returns 0 to keep cross-provider signatures uniform.</returns>
    public static async Task<int> ExecuteProcedureAsync(string host, string serviceName, string username, string password, string procedure, IDictionary<string, object?>? parameters = null, CancellationToken ct = default)
    {
        var cli = new DBAClientX.Oracle();
        await cli.ExecuteStoredProcedureAsync(host, serviceName, username, password, procedure, parameters, cancellationToken: ct).ConfigureAwait(false);
        return 0;
    }

    /// <summary>
    /// Executes a stored procedure using a full Oracle connection string.
    /// Prefer this overload when available to avoid manual connection string parsing.
    /// </summary>
    public static async Task<int> ExecuteProcedureAsync(string connectionString, string procedure, IDictionary<string, object?>? parameters = null, CancellationToken ct = default)
    {
        var b = new global::Oracle.ManagedDataAccess.Client.OracleConnectionStringBuilder(connectionString);
        var dataSource = b.DataSource ?? string.Empty;
        string host = dataSource;
        string service = dataSource;
        var slash = dataSource.LastIndexOf('/');
        if (slash > 0)
        {
            host = dataSource.Substring(0, slash);
            service = dataSource.Substring(slash + 1);
        }
        var cli = new DBAClientX.Oracle();
        await cli.ExecuteStoredProcedureAsync(host, service, b.UserID, b.Password, procedure, parameters, cancellationToken: ct).ConfigureAwait(false);
        return 0;
    }
}
