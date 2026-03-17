using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
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
    /// <summary>
    /// Default upper bound for concurrent query execution in <see cref="RunQueriesInParallel"/>.
    /// </summary>
    public const int DefaultMaxParallelQueries = 8;

    private readonly object _syncRoot = new();
    private SqlConnection? _transactionConnection;
    private SqlTransaction? _transaction;
    private string? _transactionConnectionString;
    private bool _transactionInitializing;

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
        ValidateRequiredConnectionValue(serverOrInstance, nameof(serverOrInstance), "Server");
        ValidateRequiredConnectionValue(database, nameof(database), "Database");
        if (!integratedSecurity)
        {
            ValidateRequiredConnectionValue(username, nameof(username), "Username");
        }

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

    private sealed class SqlServerParameterTypeMap : Dictionary<string, DbType>
    {
        public SqlServerParameterTypeMap(IDictionary<string, SqlDbType> providerTypes)
            : base(providerTypes.Count, StringComparer.Ordinal)
        {
            ProviderTypes = new Dictionary<string, SqlDbType>(providerTypes, StringComparer.Ordinal);
            foreach (var pair in providerTypes)
            {
                var parameter = new SqlParameter { SqlDbType = pair.Value };
                this[pair.Key] = parameter.DbType;
            }
        }

        public IReadOnlyDictionary<string, SqlDbType> ProviderTypes { get; }
    }

    private (SqlConnection Connection, SqlTransaction? Transaction, bool Dispose) ResolveConnection(string connectionString, bool useTransaction)
    {
        if (useTransaction)
        {
            lock (_syncRoot)
            {
                if (_transaction == null || _transactionConnection == null)
                {
                    throw new DbaTransactionException("Transaction has not been started.");
                }

                var normalizedConnectionString = NormalizeConnectionString(connectionString);
                if (_transactionConnectionString != null && !string.Equals(_transactionConnectionString, normalizedConnectionString, StringComparison.OrdinalIgnoreCase))
                {
                    throw new DbaTransactionException("The requested connection details do not match the active transaction.");
                }

                return (_transactionConnection, _transaction, false);
            }
        }

        var connection = CreateConnection(connectionString);
        try
        {
            OpenConnection(connection);
            return (connection, null, true);
        }
        catch
        {
            DisposeConnection(connection);
            throw;
        }
    }

    private async Task<(SqlConnection Connection, SqlTransaction? Transaction, bool Dispose)> ResolveConnectionAsync(
        string connectionString,
        bool useTransaction,
        CancellationToken cancellationToken)
    {
        if (useTransaction)
        {
            lock (_syncRoot)
            {
                if (_transaction == null || _transactionConnection == null)
                {
                    throw new DbaTransactionException("Transaction has not been started.");
                }

                var normalizedConnectionString = NormalizeConnectionString(connectionString);
                if (_transactionConnectionString != null && !string.Equals(_transactionConnectionString, normalizedConnectionString, StringComparison.OrdinalIgnoreCase))
                {
                    throw new DbaTransactionException("The requested connection details do not match the active transaction.");
                }

                return (_transactionConnection, _transaction, false);
            }
        }

        var connection = CreateConnection(connectionString);
        try
        {
            await OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);
            return (connection, null, true);
        }
        catch
        {
            DisposeConnection(connection);
            throw;
        }
    }

    /// <inheritdoc />
    protected override void AddParameters(DbCommand command, IDictionary<string, object?>? parameters, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        if (command is not SqlCommand sqlCommand || parameterTypes is not SqlServerParameterTypeMap sqlTypes)
        {
            base.AddParameters(command, parameters, parameterTypes, parameterDirections);
            return;
        }

        if (parameters == null)
        {
            return;
        }

        foreach (var pair in parameters)
        {
            var value = pair.Value ?? DBNull.Value;
            var parameter = new SqlParameter
            {
                ParameterName = pair.Key,
                Value = value
            };

            if (sqlTypes.ProviderTypes.TryGetValue(pair.Key, out var providerType))
            {
                parameter.SqlDbType = providerType;
            }
            else if (parameterTypes.TryGetValue(pair.Key, out var explicitType))
            {
                parameter.DbType = explicitType;
            }
            else
            {
                parameter.DbType = InferParameterDbType(value);
            }

            if (parameterDirections != null && parameterDirections.TryGetValue(pair.Key, out var direction))
            {
                parameter.Direction = direction;
            }

            sqlCommand.Parameters.Add(parameter);
        }
    }

    internal static IDictionary<string, DbType>? ConvertParameterTypes(IDictionary<string, SqlDbType>? types)
        => types == null ? null : new SqlServerParameterTypeMap(types);

    private static DbType InferParameterDbType(object? value)
    {
        if (value == null || value == DBNull.Value) return DbType.Object;
        if (value is Guid) return DbType.Guid;
        if (value is byte[]) return DbType.Binary;
        if (value is TimeSpan) return DbType.Time;
        if (value is DateTimeOffset) return DbType.DateTimeOffset;

        return Type.GetTypeCode(value.GetType()) switch
        {
            TypeCode.Byte => DbType.Byte,
            TypeCode.SByte => DbType.SByte,
            TypeCode.Int16 => DbType.Int16,
            TypeCode.Int32 => DbType.Int32,
            TypeCode.Int64 => DbType.Int64,
            TypeCode.UInt16 => DbType.UInt16,
            TypeCode.UInt32 => DbType.UInt32,
            TypeCode.UInt64 => DbType.UInt64,
            TypeCode.Decimal => DbType.Decimal,
            TypeCode.Double => DbType.Double,
            TypeCode.Single => DbType.Single,
            TypeCode.Boolean => DbType.Boolean,
            TypeCode.String => DbType.String,
            TypeCode.Char => DbType.StringFixedLength,
            TypeCode.DateTime => DbType.DateTime,
            _ => DbType.Object
        };
    }

    private static string NormalizeConnectionString(string connectionString)
        => new SqlConnectionStringBuilder(connectionString).ConnectionString;

    private static void ValidateRequiredConnectionValue(string? value, string paramName, string displayName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException($"{displayName} cannot be null or whitespace.", paramName);
        }
    }
}
