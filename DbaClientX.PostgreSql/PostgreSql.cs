using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace DBAClientX;

/// <summary>
/// Provides high-level convenience operations for interacting with a PostgreSQL database using the shared <see cref="DatabaseClientBase"/> abstractions.
/// </summary>
public partial class PostgreSql : DatabaseClientBase
{
    /// <summary>
    /// Default upper bound for concurrent query execution in <see cref="RunQueriesInParallel"/>.
    /// </summary>
    public const int DefaultMaxParallelQueries = 8;

    private readonly object _syncRoot = new();
    private NpgsqlConnection? _transactionConnection;
    private NpgsqlTransaction? _transaction;
    private string? _transactionConnectionString;
    private bool _transactionInitializing;

    /// <summary>
    /// Gets a value indicating whether the client currently has an active transaction scope.
    /// </summary>
    /// <remarks>
    /// The flag is toggled when <see cref="BeginTransaction(string, string, string, string)"/> or
    /// <see cref="BeginTransactionAsync(string, string, string, string, CancellationToken)"/> is invoked and returns to <see langword="false"/> after
    /// <see cref="Commit"/>, <see cref="Rollback"/>, or the async counterparts dispose the transaction.
    /// </remarks>
    public bool IsInTransaction => _transaction != null;

    /// <summary>
    /// Builds a <see cref="NpgsqlConnectionStringBuilder"/> connection string from individual connection components.
    /// </summary>
    /// <param name="host">Host name or IP address of the PostgreSQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="port">Optional TCP port; when omitted the provider default is used.</param>
    /// <param name="ssl">Optional SSL requirement flag; <see langword="true"/> enforces TLS.</param>
    /// <returns>The generated connection string.</returns>
    /// <remarks>
    /// The builder enables connection pooling by default so repeated operations reuse the same socket where possible, lowering
    /// latency and resource consumption for high-frequency workloads. Adjust pooling-related properties on the returned string
    /// when connection storm scenarios require tighter control.
    /// </remarks>
    public static string BuildConnectionString(string host, string database, string username, string password, int? port = null, bool? ssl = null)
    {
        ValidateRequiredConnectionValue(host, nameof(host), "Host");
        ValidateRequiredConnectionValue(database, nameof(database), "Database");
        ValidateRequiredConnectionValue(username, nameof(username), "Username");

        if (ssl == false)
        {
            throw new ArgumentException("PostgreSQL connections must require SSL. Pass true or omit the ssl argument.", nameof(ssl));
        }

        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = host,
            Database = database,
            Username = username,
            Password = password,
            Pooling = true,
            SslMode = SslMode.Require
        };

        if (port.HasValue)
        {
            builder.Port = port.Value;
        }

        if (ssl == true)
        {
            builder.SslMode = SslMode.Require;
        }

        return builder.ConnectionString;
    }

    /// <summary>
    /// Performs a synchronous connectivity test against the specified PostgreSQL instance.
    /// </summary>
    public virtual bool Ping(string host, string database, string username, string password)
    {
        try
        {
            ExecuteScalar(host, database, username, password, "SELECT 1");
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Performs an asynchronous connectivity test against the specified PostgreSQL instance.
    /// </summary>
    public virtual async Task<bool> PingAsync(string host, string database, string username, string password, CancellationToken cancellationToken = default)
    {
        try
        {
            await ExecuteScalarAsync(host, database, username, password, "SELECT 1", cancellationToken: cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static string NormalizeConnectionString(string connectionString)
        => new NpgsqlConnectionStringBuilder(connectionString).ConnectionString;

    private static void ValidateConnectionString(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("Connection string cannot be null or whitespace.", nameof(connectionString));
        }

        _ = NormalizeConnectionString(connectionString);
    }

    private static void ValidateRequiredConnectionValue(string value, string paramName, string displayName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException($"{displayName} cannot be null or whitespace.", paramName);
        }
    }
}
