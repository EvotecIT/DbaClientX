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
    /// <summary>Executes a parameterized SQL statement.</summary>
    /// <param name="host">Oracle host name or address.</param>
    /// <param name="serviceName">Oracle service or SID.</param>
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

    /// <summary>Executes a parameterized SQL statement using a connection string.</summary>
    /// <param name="connectionString">Oracle provider connection string.</param>
    /// <param name="sql">SQL text to execute.</param>
    /// <param name="parameters">Parameter name/value map.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Number of affected rows.</returns>
    public static Task<int> ExecuteSqlAsync(string connectionString, string sql, IDictionary<string, object?>? parameters = null, CancellationToken ct = default)
    {
        var b = new global::Oracle.ManagedDataAccess.Client.OracleConnectionStringBuilder(connectionString);
        var (host, service) = SplitDataSource(b.DataSource);
        var cli = new DBAClientX.Oracle();
        return cli.ExecuteNonQueryAsync(host, service, b.UserID, b.Password, sql, parameters, cancellationToken: ct);
    }

    // Procedure variant mirrors the provider signature
    /// <summary>Executes a stored procedure.</summary>
    /// <param name="host">Oracle host name or address.</param>
    /// <param name="serviceName">Oracle service or SID.</param>
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

    /// <summary>Executes a stored procedure using a connection string.</summary>
    /// <param name="connectionString">Oracle provider connection string.</param>
    /// <param name="procedure">Stored procedure name.</param>
    /// <param name="parameters">Parameter name/value map.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Zero. This façade returns 0 to keep cross-provider signatures uniform.</returns>
    public static async Task<int> ExecuteProcedureAsync(string connectionString, string procedure, IDictionary<string, object?>? parameters = null, CancellationToken ct = default)
    {
        var b = new global::Oracle.ManagedDataAccess.Client.OracleConnectionStringBuilder(connectionString);
        var (host, service) = SplitDataSource(b.DataSource);
        var cli = new DBAClientX.Oracle();
        await cli.ExecuteStoredProcedureAsync(host, service, b.UserID, b.Password, procedure, parameters, cancellationToken: ct).ConfigureAwait(false);
        return 0;
    }

    private static (string Host, string ServiceName) SplitDataSource(string? dataSource)
    {
        var value = dataSource ?? string.Empty;
        if (string.IsNullOrWhiteSpace(value))
        {
            return (string.Empty, string.Empty);
        }

        var slash = value.LastIndexOf('/');
        if (slash > 0 && slash < value.Length - 1)
        {
            return (value.Substring(0, slash), value.Substring(slash + 1));
        }

        return (value, value);
    }
}
