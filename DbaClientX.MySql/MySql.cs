using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using MySqlConnector;

namespace DBAClientX;

/// <summary>
/// Provides high-level convenience operations for interacting with a MySQL database using the shared <see cref="DatabaseClientBase"/> abstractions.
/// </summary>
public partial class MySql : DatabaseClientBase
{
    private readonly object _syncRoot = new();
    private MySqlConnection? _transactionConnection;
    private MySqlTransaction? _transaction;

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
    /// Builds a <see cref="MySqlConnectionStringBuilder"/> connection string from individual connection components.
    /// </summary>
    /// <param name="host">Host name or IP address of the MySQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="port">Optional TCP port; when omitted the provider default is used.</param>
    /// <param name="ssl">Optional SSL requirement flag; <see langword="true"/> enforces TLS.</param>
    /// <returns>The generated connection string.</returns>
    /// <remarks>
    /// The builder enables connection pooling by default so repeated operations reuse the same socket where possible, lowering latency and resource consumption
    /// for high-frequency workloads. Adjust pooling-related properties on the returned string when connection storm scenarios require tighter control.
    /// </remarks>
    public static string BuildConnectionString(string host, string database, string username, string password, uint? port = null, bool? ssl = null)
    {
        var builder = new MySqlConnectionStringBuilder
        {
            Server = host,
            Database = database,
            UserID = username,
            Password = password,
            Pooling = true
        };
        if (port.HasValue)
        {
            builder.Port = port.Value;
        }
        if (ssl.HasValue)
        {
            builder.SslMode = ssl.Value ? MySqlSslMode.Required : MySqlSslMode.None;
        }
        return builder.ConnectionString;
    }

    /// <summary>
    /// Performs a synchronous connectivity test against the specified MySQL instance.
    /// </summary>
    /// <param name="host">Host name or IP address of the MySQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <returns><see langword="true"/> when executing <c>SELECT 1</c> succeeds; otherwise <see langword="false"/>.</returns>
    /// <remarks>
    /// Exceptions are swallowed to keep the call lightweight; call <see cref="ExecuteScalar(string, string, string, string, string, IDictionary{string, object?}?, bool, IDictionary{string, MySqlDbType}?, IDictionary{string, ParameterDirection}?)"/> for detailed error information.
    /// </remarks>
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
    /// Performs an asynchronous connectivity test against the specified MySQL instance.
    /// </summary>
    /// <param name="host">Host name or IP address of the MySQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying query.</param>
    /// <returns><see langword="true"/> when executing <c>SELECT 1</c> succeeds; otherwise <see langword="false"/>.</returns>
    /// <remarks>
    /// The method mirrors <see cref="Ping"/> but uses async I/O primitives to avoid blocking threads.
    /// </remarks>
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
}
