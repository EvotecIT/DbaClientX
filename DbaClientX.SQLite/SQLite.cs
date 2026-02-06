using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

/// <summary>
/// Provides the SQLite-specific implementation of <see cref="DatabaseClientBase"/> exposing
/// convenience helpers for executing commands, queries, and bulk operations against a local
/// or remote SQLite database file.
/// </summary>
public partial class SQLite : DatabaseClientBase
{
    /// <summary>
    /// Default busy timeout used for operational commands when a per-instance value is not overridden.
    /// </summary>
    public const int DefaultBusyTimeoutMs = 5000;

    /// <summary>
    /// Default upper bound for concurrent query execution in <see cref="RunQueriesInParallel"/>.
    /// </summary>
    public const int DefaultMaxParallelQueries = 8;

    private readonly object _syncRoot = new();
    private SqliteConnection? _transactionConnection;
    private SqliteTransaction? _transaction;
    private bool _transactionInitializing;
    private volatile int _busyTimeoutMs = DefaultBusyTimeoutMs;

    /// <summary>
    /// Gets a value indicating whether an explicit transaction scope is currently active.
    /// </summary>
    /// <remarks>
    /// The flag is toggled by the <see cref="BeginTransaction(string)"/>,
    /// <see cref="BeginTransactionAsync(string, CancellationToken)"/> and related overloads
    /// and resets after invoking <see cref="Commit"/>, <see cref="Rollback"/> or their asynchronous counterparts.
    /// </remarks>
    public bool IsInTransaction => _transaction != null;

    /// <summary>
    /// Gets or sets the busy timeout (in milliseconds) applied to operational SQLite connections.
    /// </summary>
    /// <remarks>
    /// Set to <c>0</c> to use provider defaults and disable explicit timeout injection.
    /// </remarks>
    public int BusyTimeoutMs
    {
        get => _busyTimeoutMs;
        set
        {
            if (value < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(value), "BusyTimeoutMs cannot be negative.");
            }

            _busyTimeoutMs = value;
        }
    }

    /// <summary>
    /// Builds a connection string suitable for <see cref="SqliteConnection"/> instances using a database file path.
    /// </summary>
    /// <param name="database">Absolute or relative path of the SQLite database file.</param>
    /// <returns>A pooled connection string that targets <paramref name="database"/>.</returns>
    /// <remarks>
    /// SQLite supports connection pooling for file-backed databases. Enabling pooling minimizes the overhead of
    /// opening new connections when executing multiple commands in rapid succession. Adjust pooling-related
    /// attributes on the returned connection string if a particular workload requires more granular control.
    /// </remarks>
    public static string BuildConnectionString(string database)
        => BuildConnectionString(database, readOnly: false, busyTimeoutMs: null);

    /// <summary>
    /// Builds a connection string suitable for <see cref="SqliteConnection"/> instances using a database file path.
    /// </summary>
    /// <param name="database">Absolute or relative path of the SQLite database file.</param>
    /// <param name="readOnly">When true, opens the connection in read-only mode.</param>
    /// <param name="busyTimeoutMs">Optional busy timeout applied via the connection string (milliseconds).</param>
    /// <returns>A pooled connection string that targets <paramref name="database"/>.</returns>
    public static string BuildConnectionString(string database, bool readOnly, int? busyTimeoutMs)
    {
        var builder = new SqliteConnectionStringBuilder
        {
            DataSource = database,
            Pooling = true
        };

        if (readOnly)
        {
            builder.Mode = SqliteOpenMode.ReadOnly;
        }

        if (busyTimeoutMs.HasValue && busyTimeoutMs.Value > 0)
        {
            builder.DefaultTimeout = Math.Max(1, (int)Math.Ceiling(busyTimeoutMs.Value / 1000d));
        }

        return builder.ConnectionString;
    }

    /// <summary>
    /// Builds a read-only connection string for the supplied SQLite database.
    /// </summary>
    /// <param name="database">Absolute or relative path of the SQLite database file.</param>
    /// <param name="busyTimeoutMs">Optional busy timeout applied via the connection string (milliseconds).</param>
    /// <returns>A pooled read-only connection string.</returns>
    public static string BuildReadOnlyConnectionString(string database, int? busyTimeoutMs = null)
        => BuildConnectionString(database, readOnly: true, busyTimeoutMs: busyTimeoutMs);

    private static string BuildOperationalConnectionString(string database, bool readOnly = false) =>
        BuildConnectionString(database, readOnly, busyTimeoutMs: null);

    private int ResolveBusyTimeoutMs(int? busyTimeoutMs)
    {
        var effectiveTimeout = busyTimeoutMs ?? BusyTimeoutMs;
        if (effectiveTimeout < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(busyTimeoutMs), "Busy timeout cannot be negative.");
        }

        return effectiveTimeout;
    }

    private void ApplyBusyTimeout(SqliteConnection connection, int? busyTimeoutMs = null)
    {
        var effectiveTimeout = ResolveBusyTimeoutMs(busyTimeoutMs);
        if (effectiveTimeout <= 0)
        {
            return;
        }

        using var command = connection.CreateCommand();
        command.CommandText = $"PRAGMA busy_timeout = {effectiveTimeout};";
        command.ExecuteNonQuery();
    }

    private async Task ApplyBusyTimeoutAsync(SqliteConnection connection, int? busyTimeoutMs, CancellationToken cancellationToken)
    {
        var effectiveTimeout = ResolveBusyTimeoutMs(busyTimeoutMs);
        if (effectiveTimeout <= 0)
        {
            return;
        }

        using var command = connection.CreateCommand();
        command.CommandText = $"PRAGMA busy_timeout = {effectiveTimeout};";
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
#else
        await Task.Yield();
        command.ExecuteNonQuery();
#endif
    }

    /// <summary>
    /// Performs a lightweight connectivity test against the supplied SQLite database file.
    /// </summary>
    /// <param name="database">Absolute or relative path of the SQLite database file.</param>
    /// <returns><see langword="true"/> when executing <c>SELECT 1</c> succeeds; otherwise <see langword="false"/>.</returns>
    /// <remarks>
    /// Exceptions are intentionally swallowed so the probe can be used in health-check scenarios. When detailed
    /// error information is required, invoke <see cref="ExecuteScalar(string, string, System.Collections.Generic.IDictionary{string, object?}?, bool, System.Collections.Generic.IDictionary{string, SqliteType}?, System.Collections.Generic.IDictionary{string, System.Data.ParameterDirection}?)"/>
    /// to receive the specific <see cref="Exception"/> that occurred.
    /// </remarks>
    public virtual bool Ping(string database)
    {
        try
        {
            ExecuteScalar(database, "SELECT 1");
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Asynchronously performs a connectivity test against the supplied SQLite database file.
    /// </summary>
    /// <param name="database">Absolute or relative path of the SQLite database file.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying command execution.</param>
    /// <returns><see langword="true"/> when executing <c>SELECT 1</c> succeeds; otherwise <see langword="false"/>.</returns>
    /// <remarks>
    /// Mirrors the synchronous <see cref="Ping(string)"/> implementation while relying on asynchronous I/O to avoid
    /// blocking the caller's thread. Recommended for UI or ASP.NET workloads.
    /// </remarks>
    public virtual async Task<bool> PingAsync(string database, CancellationToken cancellationToken = default)
    {
        try
        {
            await ExecuteScalarAsync(database, "SELECT 1", cancellationToken: cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch
        {
            return false;
        }
    }
}
