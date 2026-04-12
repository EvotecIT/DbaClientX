using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DBAClientX.Invoker;

namespace DBAClientX.OracleGeneric;

/// <summary>
/// Generic, reflection-friendly façade for executing SQL or stored procedures via an Oracle connection string.
/// Internally forwards to <see cref="DBAClientX.Oracle"/>.
/// </summary>
public static class GenericExecutors
{
    internal static Func<DBAClientX.Oracle> ClientFactory { get; set; } = static () => new DBAClientX.Oracle();

    /// <summary>Executes a parameterized SQL statement.</summary>
    /// <param name="host">Oracle host name or address.</param>
    /// <param name="serviceName">Oracle service or SID.</param>
    /// <param name="username">User name.</param>
    /// <param name="password">Password.</param>
    /// <param name="sql">SQL text to execute.</param>
    /// <param name="parameters">Parameter name/value map.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Number of affected rows.</returns>
    public static async Task<int> ExecuteSqlAsync(string host, string serviceName, string username, string password, string sql, IDictionary<string, object?>? parameters = null, CancellationToken ct = default)
    {
        ValidateCommandText(sql, nameof(sql), "SQL text");
        using var cli = ClientFactory();
        return await cli.ExecuteNonQueryAsync(host, serviceName, username, password, sql, parameters, cancellationToken: ct).ConfigureAwait(false);
    }

    /// <summary>Executes a parameterized SQL statement using a connection string.</summary>
    /// <param name="connectionString">Oracle provider connection string.</param>
    /// <param name="sql">SQL text to execute.</param>
    /// <param name="parameters">Parameter name/value map.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Number of affected rows.</returns>
    public static async Task<int> ExecuteSqlAsync(string connectionString, string sql, IDictionary<string, object?>? parameters = null, CancellationToken ct = default)
    {
        ValidateConnectionString(connectionString);
        ValidateCommandText(sql, nameof(sql), "SQL text");
        using var cli = ClientFactory();
        return await cli.ExecuteNonQueryAsync(connectionString, sql, parameters, cancellationToken: ct).ConfigureAwait(false);
    }

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
        ValidateCommandText(procedure, nameof(procedure), "Stored procedure name");
        using var cli = ClientFactory();
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
        ValidateConnectionString(connectionString);
        ValidateCommandText(procedure, nameof(procedure), "Stored procedure name");
        using var cli = ClientFactory();
        await cli.ExecuteStoredProcedureAsync(connectionString, procedure, parameters, cancellationToken: ct).ConfigureAwait(false);
        return 0;
    }

    private static void ValidateConnectionString(string connectionString)
    {
        var result = DbaConnectionFactory.Validate("oracle", connectionString);
        if (!result.IsValid)
        {
            throw new ArgumentException(DbaConnectionFactory.ToUserMessage(result), nameof(connectionString));
        }
    }

    private static void ValidateCommandText(string value, string paramName, string displayName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException($"{displayName} cannot be null or whitespace.", paramName);
        }
    }
}
