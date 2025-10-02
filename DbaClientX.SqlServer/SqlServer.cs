using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

/// <summary>
/// Provides high-level convenience operations for interacting with Microsoft SQL Server using the shared
/// <see cref="DatabaseClientBase"/> infrastructure.
/// </summary>
/// <remarks>
/// The implementation mirrors the patterns exposed by the MySql provider to ensure a predictable experience when
/// switching between providers. All database access is funneled through the base class helpers so parameter handling,
/// exception wrapping, and result projection behave consistently across engines.
/// </remarks>
public partial class SqlServer : DatabaseClientBase
{
    private readonly object _syncRoot = new();
    private SqlConnection? _transactionConnection;
    private SqlTransaction? _transaction;

    /// <summary>
    /// Gets a value indicating whether a transaction scope is currently active for the client.
    /// </summary>
    /// <remarks>
    /// The property becomes <see langword="true"/> when any <c>BeginTransaction</c> overload succeeds and returns to
    /// <see langword="false"/> after <see cref="Commit"/>, <see cref="Rollback"/>, or their asynchronous counterparts
    /// complete. Consumers can poll this property to make idempotent decisions about transaction flow.
    /// </remarks>
    public bool IsInTransaction => _transaction != null;

    /// <summary>
    /// Builds a SQL Server connection string from discrete connection components.
    /// </summary>
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="port">Optional TCP port appended to <paramref name="serverOrInstance"/>.</param>
    /// <param name="ssl">Optional encryption flag routed to <see cref="SqlConnectionStringBuilder.Encrypt"/>.</param>
    /// <returns>A provider-formatted connection string.</returns>
    /// <remarks>
    /// The builder enables pooling by default to keep performance comparable to the MySQL implementation. Additional
    /// advanced options can be appended by callers if necessary for their environment.
    /// </remarks>
    public static string BuildConnectionString(
        string serverOrInstance,
        string database,
        bool integratedSecurity,
        string? username = null,
        string? password = null,
        int? port = null,
        bool? ssl = null)
    {
        var dataSource = port.HasValue ? $"{serverOrInstance},{port.Value}" : serverOrInstance;
        var connectionStringBuilder = new SqlConnectionStringBuilder
        {
            DataSource = dataSource,
            InitialCatalog = database,
            IntegratedSecurity = integratedSecurity,
            Pooling = true
        };
        if (!integratedSecurity)
        {
            connectionStringBuilder.UserID = username;
            connectionStringBuilder.Password = password;
        }
        if (ssl.HasValue)
        {
            connectionStringBuilder.Encrypt = ssl.Value;
        }
        return connectionStringBuilder.ConnectionString;
    }

    /// <summary>
    /// Performs a synchronous connectivity test against the specified SQL Server instance.
    /// </summary>
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <returns><see langword="true"/> when executing <c>SELECT 1</c> succeeds; otherwise <see langword="false"/>.</returns>
    /// <remarks>
    /// Errors are intentionally swallowed to deliver a lightweight health probe. Use <see cref="ExecuteScalar(string, string, bool, string, IDictionary{string, object?}?, bool, IDictionary{string, SqlDbType}?, IDictionary{string, ParameterDirection}?, string?, string?)"/>
    /// when detailed exception information is required.
    /// </remarks>
    public virtual bool Ping(
        string serverOrInstance,
        string database,
        bool integratedSecurity,
        string? username = null,
        string? password = null)
    {
        try
        {
            ExecuteScalar(serverOrInstance, database, integratedSecurity, "SELECT 1", username: username, password: password);
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Asynchronously performs a connectivity test against the specified SQL Server instance.
    /// </summary>
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying query.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <returns><see langword="true"/> when executing <c>SELECT 1</c> succeeds; otherwise <see langword="false"/>.</returns>
    /// <remarks>
    /// Mirrors <see cref="Ping"/> but leverages async I/O primitives to avoid blocking threads in scalable environments.
    /// </remarks>
    public virtual async Task<bool> PingAsync(
        string serverOrInstance,
        string database,
        bool integratedSecurity,
        CancellationToken cancellationToken = default,
        string? username = null,
        string? password = null)
    {
        try
        {
            await ExecuteScalarAsync(serverOrInstance, database, integratedSecurity, "SELECT 1", cancellationToken: cancellationToken, username: username, password: password).ConfigureAwait(false);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private SqlConnection ResolveConnection(string connectionString, bool useTransaction, out bool dispose)
    {
        if (useTransaction)
        {
            if (_transaction == null || _transactionConnection == null)
            {
                throw new DbaTransactionException("Transaction has not been started.");
            }

            dispose = false;
            return _transactionConnection;
        }

        var connection = new SqlConnection(connectionString);
        connection.Open();
        dispose = true;
        return connection;
    }

    private async Task<(SqlConnection Connection, bool Dispose)> ResolveConnectionAsync(
        string connectionString,
        bool useTransaction,
        CancellationToken cancellationToken)
    {
        if (useTransaction)
        {
            if (_transaction == null || _transactionConnection == null)
            {
                throw new DbaTransactionException("Transaction has not been started.");
            }

            return (_transactionConnection, false);
        }

        var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        return (connection, true);
    }

    private static IDictionary<string, DbType>? ConvertParameterTypes(IDictionary<string, SqlDbType>? types) =>
        DbTypeConverter.ConvertParameterTypes(types, static () => new SqlParameter(), static (p, t) => p.SqlDbType = t);
}
