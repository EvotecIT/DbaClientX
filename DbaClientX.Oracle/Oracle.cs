using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;

namespace DBAClientX;

/// <summary>
/// Provides high-level convenience operations for interacting with an Oracle database using the shared
/// <see cref="DatabaseClientBase"/> abstractions.
/// </summary>
public partial class Oracle : DatabaseClientBase
{
    private readonly object _syncRoot = new();
    private OracleConnection? _transactionConnection;
    private OracleTransaction? _transaction;

    /// <summary>
    /// Gets a value indicating whether the client currently has an active transaction scope.
    /// </summary>
    /// <remarks>
    /// The flag is toggled when <see cref="BeginTransaction(string, string, string, string)"/> or
    /// <see cref="BeginTransactionAsync(string, string, string, string, CancellationToken)"/> is invoked and
    /// returns to <see langword="false"/> after <see cref="Commit"/>, <see cref="Rollback"/>, or the async counterparts dispose the
    /// transaction.
    /// </remarks>
    public bool IsInTransaction => _transaction != null;

    /// <summary>
    /// Builds a <see cref="OracleConnectionStringBuilder"/> connection string from individual connection components.
    /// </summary>
    /// <param name="host">Host name or IP address of the Oracle listener.</param>
    /// <param name="serviceName">Oracle service name or SID.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="port">Optional TCP port; when omitted the provider default is used.</param>
    /// <returns>The generated connection string.</returns>
    /// <remarks>
    /// The builder enables connection pooling by default so repeated operations reuse the same socket where possible, lowering latency and
    /// resource consumption for high-frequency workloads. Adjust pooling-related properties on the returned string when connection storm scenarios
    /// require tighter control.
    /// </remarks>
    public static string BuildConnectionString(string host, string serviceName, string username, string password, int? port = null)
    {
        var builder = new OracleConnectionStringBuilder
        {
            DataSource = port.HasValue ? $"{host}:{port}/{serviceName}" : $"{host}/{serviceName}",
            UserID = username,
            Password = password,
            Pooling = true
        };
        return builder.ConnectionString;
    }

    /// <summary>
    /// Performs a synchronous connectivity test against the specified Oracle instance.
    /// </summary>
    /// <param name="host">Host name or IP address of the Oracle listener.</param>
    /// <param name="serviceName">Oracle service name or SID.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <returns><see langword="true"/> when executing <c>SELECT 1 FROM dual</c> succeeds; otherwise <see langword="false"/>.</returns>
    /// <remarks>
    /// Exceptions are swallowed to keep the call lightweight; call
    /// <see cref="ExecuteScalar(string, string, string, string, string, IDictionary{string, object?}?, bool, IDictionary{string, OracleDbType}?, IDictionary{string, ParameterDirection}?)"/>
    /// for detailed error information.
    /// </remarks>
    public virtual bool Ping(string host, string serviceName, string username, string password)
    {
        try
        {
            ExecuteScalar(host, serviceName, username, password, "SELECT 1 FROM dual");
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Performs an asynchronous connectivity test against the specified Oracle instance.
    /// </summary>
    /// <param name="host">Host name or IP address of the Oracle listener.</param>
    /// <param name="serviceName">Oracle service name or SID.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying network call.</param>
    /// <returns><see langword="true"/> when executing <c>SELECT 1 FROM dual</c> succeeds; otherwise <see langword="false"/>.</returns>
    /// <remarks>
    /// Exceptions are swallowed to keep the call lightweight; call
    /// <see cref="ExecuteScalarAsync(string, string, string, string, string, IDictionary{string, object?}?, bool, CancellationToken, IDictionary{string, OracleDbType}?, IDictionary{string, ParameterDirection}?)"/>
    /// for detailed error information.
    /// </remarks>
    public virtual async Task<bool> PingAsync(string host, string serviceName, string username, string password, CancellationToken cancellationToken = default)
    {
        try
        {
            await ExecuteScalarAsync(host, serviceName, username, password, "SELECT 1 FROM dual", cancellationToken: cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch
        {
            return false;
        }
    }
}
