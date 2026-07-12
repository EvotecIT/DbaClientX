using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DBAClientX.Invoker;

namespace DBAClientX.SQLiteGeneric;

/// <summary>
/// Provides reflection-friendly helpers that forward to <see cref="DBAClientX.SQLite"/> using either a SQLite connection string
/// or a file path. The methods are designed for scenarios where dependency injection or direct instantiation of the provider is
/// impractical, such as in scripting environments.
/// </summary>
public static class GenericExecutors
{
    internal static Func<DBAClientX.SQLite> ClientFactory { get; set; } = static () => new DBAClientX.SQLite();

    /// <summary>
    /// Executes a parameterized SQL statement against the provided database.
    /// </summary>
    /// <param name="connectionStringOrPath">Either a full SQLite connection string or a database file path.</param>
    /// <param name="sql">SQL text to execute.</param>
    /// <param name="parameters">Optional map containing parameter names and values.</param>
    /// <param name="ct">Token used to cancel the operation.</param>
    /// <returns>A task producing the number of rows affected by the command.</returns>
    /// <remarks>
    /// The helper instantiates and asynchronously disposes a new <see cref="DBAClientX.SQLite"/> instance for each invocation.
    /// Paths use <see cref="DBAClientX.SQLite.ExecuteNonQueryAsync"/>; full connection strings use
    /// <see cref="DBAClientX.SQLite.ExecuteNonQueryWithConnectionStringAsync"/> so provider options are preserved.
    /// </remarks>
    public static async Task<int> ExecuteSqlAsync(string connectionStringOrPath, string sql, IDictionary<string, object?>? parameters = null, CancellationToken ct = default)
    {
        ValidateConnectionStringOrPath(connectionStringOrPath);
        ValidateCommandText(sql, nameof(sql), "SQL text");
        await using var client = ClientFactory();
        if (DBAClientX.SQLite.IsConnectionString(connectionStringOrPath))
        {
            ValidateConnectionString(connectionStringOrPath);
            return await client.ExecuteNonQueryWithConnectionStringAsync(
                connectionStringOrPath,
                sql,
                parameters,
                cancellationToken: ct).ConfigureAwait(false);
        }

        return await client.ExecuteNonQueryAsync(
            connectionStringOrPath,
            sql,
            parameters,
            cancellationToken: ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Not supported because SQLite does not implement stored procedure semantics.
    /// </summary>
    /// <exception cref="NotSupportedException">Always thrown.</exception>
    public static Task<int> ExecuteProcedureAsync(string connectionStringOrPath, string procedure, IDictionary<string, object?>? parameters = null, CancellationToken ct = default)
        => Task.FromException<int>(new NotSupportedException("SQLite does not support stored procedures."));

    private static void ValidateConnectionStringOrPath(string connectionStringOrPath)
    {
        if (string.IsNullOrWhiteSpace(connectionStringOrPath))
        {
            throw new ArgumentException("Connection string or database path cannot be null or whitespace.", nameof(connectionStringOrPath));
        }
    }

    private static void ValidateConnectionString(string connectionString)
    {
        var validationResult = DbaConnectionFactory.Validate("sqlite", connectionString);
        if (!validationResult.IsValid)
        {
            throw new ArgumentException(DbaConnectionFactory.ToUserMessage(validationResult), "connectionStringOrPath");
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
