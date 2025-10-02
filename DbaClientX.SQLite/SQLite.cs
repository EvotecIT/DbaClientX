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
    private readonly object _syncRoot = new();
    private SqliteConnection? _transactionConnection;
    private SqliteTransaction? _transaction;
    private bool _transactionInitializing;

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
    {
        return new SqliteConnectionStringBuilder
        {
            DataSource = database,
            Pooling = true
        }.ConnectionString;
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
