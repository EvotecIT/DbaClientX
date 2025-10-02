using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
using System.Runtime.CompilerServices;
#endif

namespace DBAClientX;

/// <summary>
/// Provides high-level convenience operations for interacting with an Oracle database using the shared
/// <see cref="DatabaseClientBase"/> abstractions.
/// </summary>
public class Oracle : DatabaseClientBase
{
    private readonly object _syncRoot = new();
    private OracleConnection? _transactionConnection;
    private OracleTransaction? _transaction;

    /// <summary>
    /// Gets a value indicating whether the client currently has an active transaction scope.
    /// </summary>
    /// <remarks>
    /// The flag is toggled when <see cref="BeginTransaction(string, string, string, string)"/> or
    /// <see cref="BeginTransactionAsync(string, string, string, string, System.Threading.CancellationToken)"/> is invoked and
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
    /// The builder enables connection pooling by default so repeated operations reuse the same socket where possible, lowering
    /// latency and resource consumption for high-frequency workloads. Adjust pooling-related properties on the returned string when
    /// connection storm scenarios require tighter control.
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

    private static IDictionary<string, DbType>? ConvertParameterTypes(IDictionary<string, OracleDbType>? types) =>
        DbTypeConverter.ConvertParameterTypes(types, static () => new OracleParameter(), static (p, t) => p.OracleDbType = t);

    /// <summary>
    /// Executes a SQL query and materializes the results into the default <see cref="DatabaseClientBase"/> return format.
    /// </summary>
    /// <param name="host">Host name or IP address of the Oracle listener.</param>
    /// <param name="serviceName">Oracle service name or SID.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional parameter bag matching Oracle parameter names to values.</param>
    /// <param name="useTransaction"><see langword="true"/> to reuse the ambient transaction started by <see cref="BeginTransaction(string, string, string, string)"/>.</param>
    /// <param name="parameterTypes">Optional parameter type hints keyed by parameter name.</param>
    /// <param name="parameterDirections">Optional direction overrides keyed by parameter name.</param>
    /// <returns>A provider-agnostic result object composed by <see cref="DatabaseClientBase.BuildResult(DataSet)"/>.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction is active.</exception>
    /// <exception cref="DbaQueryExecutionException">Wraps any Oracle exception encountered during execution.</exception>
    public virtual object? Query(string host, string serviceName, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, OracleDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, serviceName, username, password);
        OracleConnection? connection = null;
        bool dispose = false;
        try
        {
            if (useTransaction)
            {
                if (_transaction == null || _transactionConnection == null)
                {
                    throw new DbaTransactionException("Transaction has not been started.");
                }
                connection = _transactionConnection;
            }
            else
            {
                connection = new OracleConnection(connectionString);
                connection.Open();
                dispose = true;
            }
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return ExecuteQuery(connection, useTransaction ? _transaction : null, query, parameters, dbTypes, parameterDirections);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute query.", query, ex);
        }
        finally
        {
            if (dispose)
            {
                connection?.Dispose();
            }
        }
    }

    /// <summary>
    /// Asynchronously executes a SQL query and materializes the results into the default <see cref="DatabaseClientBase"/> return format.
    /// </summary>
    /// <param name="host">Host name or IP address of the Oracle listener.</param>
    /// <param name="serviceName">Oracle service name or SID.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional parameter bag matching Oracle parameter names to values.</param>
    /// <param name="useTransaction"><see langword="true"/> to reuse the ambient transaction started by <see cref="BeginTransaction(string, string, string, string)"/>.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying Oracle command.</param>
    /// <param name="parameterTypes">Optional parameter type hints keyed by parameter name.</param>
    /// <param name="parameterDirections">Optional direction overrides keyed by parameter name.</param>
    /// <returns>A task producing the provider-agnostic result object composed by <see cref="DatabaseClientBase.BuildResult(DataSet)"/>.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction is active.</exception>
    /// <exception cref="DbaQueryExecutionException">Wraps any Oracle exception encountered during execution.</exception>
    public virtual async Task<object?> QueryAsync(string host, string serviceName, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, OracleDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, serviceName, username, password);
        OracleConnection? connection = null;
        bool dispose = false;
        try
        {
            if (useTransaction)
            {
                if (_transaction == null || _transactionConnection == null)
                {
                    throw new DbaTransactionException("Transaction has not been started.");
                }
                connection = _transactionConnection;
            }
            else
            {
                connection = new OracleConnection(connectionString);
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                dispose = true;
            }
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return await ExecuteQueryAsync(connection, useTransaction ? _transaction : null, query, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute query.", query, ex);
        }
        finally
        {
            if (dispose)
            {
                connection?.Dispose();
            }
        }
    }

    /// <summary>
    /// Executes a SQL query and returns the first column of the first row in the result set.
    /// </summary>
    /// <param name="host">Host name or IP address of the Oracle listener.</param>
    /// <param name="serviceName">Oracle service name or SID.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional parameter bag matching Oracle parameter names to values.</param>
    /// <param name="useTransaction"><see langword="true"/> to reuse the ambient transaction started by <see cref="BeginTransaction(string, string, string, string)"/>.</param>
    /// <param name="parameterTypes">Optional parameter type hints keyed by parameter name.</param>
    /// <param name="parameterDirections">Optional direction overrides keyed by parameter name.</param>
    /// <returns>The first column of the first row in the result set, or <see langword="null"/> when no rows are returned.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction is active.</exception>
    /// <exception cref="DbaQueryExecutionException">Wraps any Oracle exception encountered during execution.</exception>
    public virtual object? ExecuteScalar(string host, string serviceName, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, OracleDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, serviceName, username, password);
        OracleConnection? connection = null;
        bool dispose = false;
        try
        {
            if (useTransaction)
            {
                if (_transaction == null || _transactionConnection == null)
                {
                    throw new DbaTransactionException("Transaction has not been started.");
                }
                connection = _transactionConnection;
            }
            else
            {
                connection = new OracleConnection(connectionString);
                connection.Open();
                dispose = true;
            }
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return ExecuteScalar(connection, useTransaction ? _transaction : null, query, parameters, dbTypes, parameterDirections);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute scalar query.", query, ex);
        }
        finally
        {
            if (dispose)
            {
                connection?.Dispose();
            }
        }
    }

    /// <summary>
    /// Asynchronously executes a SQL query and returns the first column of the first row in the result set.
    /// </summary>
    /// <param name="host">Host name or IP address of the Oracle listener.</param>
    /// <param name="serviceName">Oracle service name or SID.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional parameter bag matching Oracle parameter names to values.</param>
    /// <param name="useTransaction"><see langword="true"/> to reuse the ambient transaction started by <see cref="BeginTransaction(string, string, string, string)"/>.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying Oracle command.</param>
    /// <param name="parameterTypes">Optional parameter type hints keyed by parameter name.</param>
    /// <param name="parameterDirections">Optional direction overrides keyed by parameter name.</param>
    /// <returns>A task producing the first column of the first row in the result set, or <see langword="null"/> when no rows are returned.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction is active.</exception>
    /// <exception cref="DbaQueryExecutionException">Wraps any Oracle exception encountered during execution.</exception>
    public virtual async Task<object?> ExecuteScalarAsync(string host, string serviceName, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, OracleDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, serviceName, username, password);
        OracleConnection? connection = null;
        bool dispose = false;
        try
        {
            if (useTransaction)
            {
                if (_transaction == null || _transactionConnection == null)
                {
                    throw new DbaTransactionException("Transaction has not been started.");
                }
                connection = _transactionConnection;
            }
            else
            {
                connection = new OracleConnection(connectionString);
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                dispose = true;
            }
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return await ExecuteScalarAsync(connection, useTransaction ? _transaction : null, query, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute scalar query.", query, ex);
        }
        finally
        {
            if (dispose)
            {
                connection?.Dispose();
            }
        }
    }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
    /// <summary>
    /// Streams the results of a SQL query as an asynchronous sequence of <see cref="DataRow"/> instances.
    /// </summary>
    /// <param name="host">Host name or IP address of the Oracle listener.</param>
    /// <param name="serviceName">Oracle service name or SID.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional parameter bag matching Oracle parameter names to values.</param>
    /// <param name="useTransaction"><see langword="true"/> to reuse the ambient transaction started by <see cref="BeginTransaction(string, string, string, string)"/>.</param>
    /// <param name="cancellationToken">Token used to cancel enumeration.</param>
    /// <param name="parameterTypes">Optional parameter type hints keyed by parameter name.</param>
    /// <param name="parameterDirections">Optional direction overrides keyed by parameter name.</param>
    /// <returns>An asynchronous stream of <see cref="DataRow"/> objects representing the results.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction is active.</exception>
    /// <remarks>
    /// Use this overload when result sets are too large to buffer in memory. The enumerable wraps <see cref="DatabaseClientBase.ExecuteQueryStreamAsync(DbConnection, DbTransaction?, string, IDictionary{string, object?}?, CancellationToken, IDictionary{string, DbType}?, IDictionary{string, ParameterDirection}?, IEnumerable{DbParameter}?, CommandType)"/>,
    /// keeping the Oracle connection open until enumeration finishes. Dispose the enumerator or exhaust the stream to release resources promptly.
    /// </remarks>
    public virtual IAsyncEnumerable<DataRow> QueryStreamAsync(string host, string serviceName, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, [EnumeratorCancellation] CancellationToken cancellationToken = default, IDictionary<string, OracleDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        return Stream();

        async IAsyncEnumerable<DataRow> Stream()
        {
            var connectionString = BuildConnectionString(host, serviceName, username, password);

            OracleConnection? connection = null;
            bool dispose = false;
            if (useTransaction)
            {
                if (_transaction == null || _transactionConnection == null)
                {
                    throw new DbaTransactionException("Transaction has not been started.");
                }
                connection = _transactionConnection;
            }
            else
            {
                connection = new OracleConnection(connectionString);
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                dispose = true;
            }

            var dbTypes = ConvertParameterTypes(parameterTypes);
            try
            {
                await foreach (var row in ExecuteQueryStreamAsync(connection, useTransaction ? _transaction : null, query, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false))
                {
                    yield return row;
                }
            }
            finally
            {
                if (dispose)
                {
                    connection?.Dispose();
                }
            }
        }
    }

    /// <summary>
    /// Streams the results of an Oracle stored procedure execution as <see cref="DataRow"/> instances.
    /// </summary>
    /// <param name="host">Host name or IP address of the Oracle listener.</param>
    /// <param name="serviceName">Oracle service name or SID.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="procedure">Stored procedure name.</param>
    /// <param name="parameters">Optional parameter bag matching Oracle parameter names to values.</param>
    /// <param name="useTransaction"><see langword="true"/> to reuse the ambient transaction started by <see cref="BeginTransaction(string, string, string, string)"/>.</param>
    /// <param name="cancellationToken">Token used to cancel enumeration.</param>
    /// <param name="parameterTypes">Optional parameter type hints keyed by parameter name.</param>
    /// <param name="parameterDirections">Optional direction overrides keyed by parameter name.</param>
    /// <returns>An asynchronous stream of <see cref="DataRow"/> objects representing the procedure result sets.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction is active.</exception>
    /// <remarks>
    /// Stream stored procedure output when large REF CURSOR payloads or long-running batches would otherwise exhaust memory. The enumerable delegates to <see cref="DatabaseClientBase.ExecuteQueryStreamAsync(DbConnection, DbTransaction?, string, IDictionary{string, object?}?, CancellationToken, IDictionary{string, DbType}?, IDictionary{string, ParameterDirection}?, IEnumerable{DbParameter}?, CommandType)"/>,
    /// keeping the Oracle connection open until enumeration finishes. Dispose the enumerator or exhaust the stream to release resources promptly.
    /// </remarks>
    public virtual IAsyncEnumerable<DataRow> ExecuteStoredProcedureStreamAsync(string host, string serviceName, string username, string password, string procedure, IDictionary<string, object?>? parameters = null, bool useTransaction = false, [EnumeratorCancellation] CancellationToken cancellationToken = default, IDictionary<string, OracleDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        return Stream();

        async IAsyncEnumerable<DataRow> Stream()
        {
            var connectionString = BuildConnectionString(host, serviceName, username, password);

            OracleConnection? connection = null;
            bool dispose = false;
            if (useTransaction)
            {
                if (_transaction == null || _transactionConnection == null)
                {
                    throw new DbaTransactionException("Transaction has not been started.");
                }
                connection = _transactionConnection;
            }
            else
            {
                connection = new OracleConnection(connectionString);
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                dispose = true;
            }

            var dbTypes = ConvertParameterTypes(parameterTypes);
            try
            {
                await foreach (var row in ExecuteQueryStreamAsync(connection, useTransaction ? _transaction : null, procedure, parameters, cancellationToken, dbTypes, parameterDirections, commandType: CommandType.StoredProcedure).ConfigureAwait(false))
                {
                    yield return row;
                }
            }
            finally
            {
                if (dispose)
                {
                    connection?.Dispose();
                }
            }
        }
    }

    /// <summary>
    /// Streams the results of an Oracle stored procedure using pre-built <see cref="DbParameter"/> instances.
    /// </summary>
    /// <param name="host">Host name or IP address of the Oracle listener.</param>
    /// <param name="serviceName">Oracle service name or SID.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="procedure">Stored procedure name.</param>
    /// <param name="parameters">Optional collection of preconfigured parameters to send to Oracle.</param>
    /// <param name="useTransaction"><see langword="true"/> to reuse the ambient transaction started by <see cref="BeginTransaction(string, string, string, string)"/>.</param>
    /// <param name="cancellationToken">Token used to cancel enumeration.</param>
    /// <returns>An asynchronous stream of <see cref="DataRow"/> objects representing the procedure result sets.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction is active.</exception>
    /// <remarks>
    /// Use this overload when advanced Oracle data types or parameter binding scenarios require manually constructed parameters. The enumerable keeps the connection open until enumeration completes.
    /// </remarks>
    public virtual IAsyncEnumerable<DataRow> ExecuteStoredProcedureStreamAsync(string host, string serviceName, string username, string password, string procedure, IEnumerable<DbParameter>? parameters = null, bool useTransaction = false, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        return Stream();

        async IAsyncEnumerable<DataRow> Stream()
        {
            var connectionString = BuildConnectionString(host, serviceName, username, password);

            OracleConnection? connection = null;
            bool dispose = false;
            if (useTransaction)
            {
                if (_transaction == null || _transactionConnection == null)
                {
                    throw new DbaTransactionException("Transaction has not been started.");
                }
                connection = _transactionConnection;
            }
            else
            {
                connection = new OracleConnection(connectionString);
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                dispose = true;
            }

            try
            {
                await foreach (var row in ExecuteQueryStreamAsync(connection, useTransaction ? _transaction : null, procedure, cancellationToken: cancellationToken, dbParameters: parameters, commandType: CommandType.StoredProcedure).ConfigureAwait(false))
                {
                    yield return row;
                }
            }
            finally
            {
                if (dispose)
                {
                    connection?.Dispose();
                }
            }
        }
    }
#endif

    /// <summary>
    /// Executes a SQL statement that does not return rows, such as INSERT, UPDATE, or DELETE.
    /// </summary>
    /// <param name="host">Host name or IP address of the Oracle listener.</param>
    /// <param name="serviceName">Oracle service name or SID.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional parameter bag matching Oracle parameter names to values.</param>
    /// <param name="useTransaction"><see langword="true"/> to reuse the ambient transaction started by <see cref="BeginTransaction(string, string, string, string)"/>.</param>
    /// <param name="parameterTypes">Optional parameter type hints keyed by parameter name.</param>
    /// <param name="parameterDirections">Optional direction overrides keyed by parameter name.</param>
    /// <returns>The number of rows affected.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction is active.</exception>
    /// <exception cref="DbaQueryExecutionException">Wraps any Oracle exception encountered during execution.</exception>
    public virtual int ExecuteNonQuery(string host, string serviceName, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, OracleDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, serviceName, username, password);
        OracleConnection? connection = null;
        bool dispose = false;
        try
        {
            if (useTransaction)
            {
                if (_transaction == null || _transactionConnection == null)
                {
                    throw new DbaTransactionException("Transaction has not been started.");
                }
                connection = _transactionConnection;
            }
            else
            {
                connection = new OracleConnection(connectionString);
                connection.Open();
                dispose = true;
            }
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return ExecuteNonQuery(connection, useTransaction ? _transaction : null, query, parameters, dbTypes, parameterDirections);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute non-query.", query, ex);
        }
        finally
        {
            if (dispose)
            {
                connection?.Dispose();
            }
        }
    }

    /// <summary>
    /// Asynchronously executes a SQL statement that does not return rows, such as INSERT, UPDATE, or DELETE.
    /// </summary>
    /// <param name="host">Host name or IP address of the Oracle listener.</param>
    /// <param name="serviceName">Oracle service name or SID.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional parameter bag matching Oracle parameter names to values.</param>
    /// <param name="useTransaction"><see langword="true"/> to reuse the ambient transaction started by <see cref="BeginTransaction(string, string, string, string)"/>.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying Oracle command.</param>
    /// <param name="parameterTypes">Optional parameter type hints keyed by parameter name.</param>
    /// <param name="parameterDirections">Optional direction overrides keyed by parameter name.</param>
    /// <returns>A task producing the number of rows affected.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction is active.</exception>
    /// <exception cref="DbaQueryExecutionException">Wraps any Oracle exception encountered during execution.</exception>
    public virtual async Task<int> ExecuteNonQueryAsync(string host, string serviceName, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, OracleDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, serviceName, username, password);
        OracleConnection? connection = null;
        bool dispose = false;
        try
        {
            if (useTransaction)
            {
                if (_transaction == null || _transactionConnection == null)
                {
                    throw new DbaTransactionException("Transaction has not been started.");
                }
                connection = _transactionConnection;
            }
            else
            {
                connection = new OracleConnection(connectionString);
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                dispose = true;
            }
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return await ExecuteNonQueryAsync(connection, useTransaction ? _transaction : null, query, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute non-query.", query, ex);
        }
        finally
        {
            if (dispose)
            {
                connection?.Dispose();
            }
        }
    }

    /// <summary>
    /// Executes an Oracle stored procedure and materializes the results into the default <see cref="DatabaseClientBase"/> return format.
    /// </summary>
    /// <param name="host">Host name or IP address of the Oracle listener.</param>
    /// <param name="serviceName">Oracle service name or SID.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="procedure">Stored procedure name.</param>
    /// <param name="parameters">Optional parameter bag matching Oracle parameter names to values.</param>
    /// <param name="useTransaction"><see langword="true"/> to reuse the ambient transaction started by <see cref="BeginTransaction(string, string, string, string)"/>.</param>
    /// <param name="parameterTypes">Optional parameter type hints keyed by parameter name.</param>
    /// <param name="parameterDirections">Optional direction overrides keyed by parameter name.</param>
    /// <returns>A provider-agnostic result object composed by <see cref="DatabaseClientBase.BuildResult(DataSet)"/>.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction is active.</exception>
    /// <exception cref="DbaQueryExecutionException">Wraps any Oracle exception encountered during execution.</exception>
    public virtual object? ExecuteStoredProcedure(string host, string serviceName, string username, string password, string procedure, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, OracleDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, serviceName, username, password);
        OracleConnection? connection = null;
        bool dispose = false;
        try
        {
            if (useTransaction)
            {
                if (_transaction == null || _transactionConnection == null)
                {
                    throw new DbaTransactionException("Transaction has not been started.");
                }
                connection = _transactionConnection;
            }
            else
            {
                connection = new OracleConnection(connectionString);
                connection.Open();
                dispose = true;
            }
            using var command = connection.CreateCommand();
            command.CommandText = procedure;
            command.CommandType = CommandType.StoredProcedure;
            command.Transaction = useTransaction ? _transaction : null;
            var dbTypes = ConvertParameterTypes(parameterTypes);
            AddParameters(command, parameters, dbTypes, parameterDirections);
            var commandTimeout = CommandTimeout;
            if (commandTimeout > 0)
            {
                command.CommandTimeout = commandTimeout;
            }
            var dataSet = new DataSet();
            using var reader = command.ExecuteReader();
            var tableIndex = 0;
            do
            {
                var table = new DataTable($"Table{tableIndex}");
                table.Load(reader);
                dataSet.Tables.Add(table);
                tableIndex++;
            } while (!reader.IsClosed && reader.NextResult());
            var result = BuildResult(dataSet);
            UpdateOutputParameters(command, parameters);
            return result;
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute stored procedure.", procedure, ex);
        }
        finally
        {
            if (dispose)
            {
                connection?.Dispose();
            }
        }
    }

    /// <summary>
    /// Asynchronously executes an Oracle stored procedure and materializes the results into the default <see cref="DatabaseClientBase"/> return format.
    /// </summary>
    /// <param name="host">Host name or IP address of the Oracle listener.</param>
    /// <param name="serviceName">Oracle service name or SID.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="procedure">Stored procedure name.</param>
    /// <param name="parameters">Optional parameter bag matching Oracle parameter names to values.</param>
    /// <param name="useTransaction"><see langword="true"/> to reuse the ambient transaction started by <see cref="BeginTransaction(string, string, string, string)"/>.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying Oracle command.</param>
    /// <param name="parameterTypes">Optional parameter type hints keyed by parameter name.</param>
    /// <param name="parameterDirections">Optional direction overrides keyed by parameter name.</param>
    /// <returns>A task producing the provider-agnostic result object composed by <see cref="DatabaseClientBase.BuildResult(DataSet)"/>.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction is active.</exception>
    /// <exception cref="DbaQueryExecutionException">Wraps any Oracle exception encountered during execution.</exception>
    public virtual async Task<object?> ExecuteStoredProcedureAsync(string host, string serviceName, string username, string password, string procedure, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, OracleDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, serviceName, username, password);
        OracleConnection? connection = null;
        bool dispose = false;
        try
        {
            if (useTransaction)
            {
                if (_transaction == null || _transactionConnection == null)
                {
                    throw new DbaTransactionException("Transaction has not been started.");
                }
                connection = _transactionConnection;
            }
            else
            {
                connection = new OracleConnection(connectionString);
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                dispose = true;
            }
            using var command = connection.CreateCommand();
            command.CommandText = procedure;
            command.CommandType = CommandType.StoredProcedure;
            command.Transaction = useTransaction ? _transaction : null;
            var dbTypes = ConvertParameterTypes(parameterTypes);
            AddParameters(command, parameters, dbTypes, parameterDirections);
            var commandTimeout = CommandTimeout;
            if (commandTimeout > 0)
            {
                command.CommandTimeout = commandTimeout;
            }
            var dataSet = new DataSet();
            using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            var tableIndex = 0;
            do
            {
                var table = new DataTable($"Table{tableIndex}");
                table.Load(reader);
                dataSet.Tables.Add(table);
                tableIndex++;
            } while (!reader.IsClosed && await reader.NextResultAsync(cancellationToken).ConfigureAwait(false));
            var result = BuildResult(dataSet);
            UpdateOutputParameters(command, parameters);
            return result;
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute stored procedure.", procedure, ex);
        }
        finally
        {
            if (dispose)
            {
                connection?.Dispose();
            }
        }
    }

    /// <summary>
    /// Executes an Oracle stored procedure using pre-built <see cref="DbParameter"/> instances.
    /// </summary>
    /// <param name="host">Host name or IP address of the Oracle listener.</param>
    /// <param name="serviceName">Oracle service name or SID.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="procedure">Stored procedure name.</param>
    /// <param name="parameters">Optional collection of preconfigured parameters to send to Oracle.</param>
    /// <param name="useTransaction"><see langword="true"/> to reuse the ambient transaction started by <see cref="BeginTransaction(string, string, string, string)"/>.</param>
    /// <returns>A provider-agnostic result object composed by <see cref="DatabaseClientBase.BuildResult(DataSet)"/>.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction is active.</exception>
    /// <exception cref="DbaQueryExecutionException">Wraps any Oracle exception encountered during execution.</exception>
    public virtual object? ExecuteStoredProcedure(string host, string serviceName, string username, string password, string procedure, IEnumerable<DbParameter>? parameters = null, bool useTransaction = false)
    {
        var connectionString = BuildConnectionString(host, serviceName, username, password);
        OracleConnection? connection = null;
        bool dispose = false;
        try
        {
            if (useTransaction)
            {
                if (_transaction == null || _transactionConnection == null)
                {
                    throw new DbaTransactionException("Transaction has not been started.");
                }
                connection = _transactionConnection;
            }
            else
            {
                connection = new OracleConnection(connectionString);
                connection.Open();
                dispose = true;
            }
            using var command = connection.CreateCommand();
            command.CommandText = procedure;
            command.CommandType = CommandType.StoredProcedure;
            command.Transaction = useTransaction ? _transaction : null;
            AddParameters(command, parameters);
            var commandTimeout = CommandTimeout;
            if (commandTimeout > 0)
            {
                command.CommandTimeout = commandTimeout;
            }
            var dataSet = new DataSet();
            using var reader = command.ExecuteReader();
            var tableIndex = 0;
            do
            {
                var table = new DataTable($"Table{tableIndex}");
                table.Load(reader);
                dataSet.Tables.Add(table);
                tableIndex++;
            } while (!reader.IsClosed && reader.NextResult());
            return BuildResult(dataSet);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute stored procedure.", procedure, ex);
        }
        finally
        {
            if (dispose)
            {
                connection?.Dispose();
            }
        }
    }

    /// <summary>
    /// Asynchronously executes an Oracle stored procedure using pre-built <see cref="DbParameter"/> instances.
    /// </summary>
    /// <param name="host">Host name or IP address of the Oracle listener.</param>
    /// <param name="serviceName">Oracle service name or SID.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="procedure">Stored procedure name.</param>
    /// <param name="parameters">Optional collection of preconfigured parameters to send to Oracle.</param>
    /// <param name="useTransaction"><see langword="true"/> to reuse the ambient transaction started by <see cref="BeginTransaction(string, string, string, string)"/>.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying Oracle command.</param>
    /// <returns>A task producing the provider-agnostic result object composed by <see cref="DatabaseClientBase.BuildResult(DataSet)"/>.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction is active.</exception>
    /// <exception cref="DbaQueryExecutionException">Wraps any Oracle exception encountered during execution.</exception>
    public virtual async Task<object?> ExecuteStoredProcedureAsync(string host, string serviceName, string username, string password, string procedure, IEnumerable<DbParameter>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default)
    {
        var connectionString = BuildConnectionString(host, serviceName, username, password);
        OracleConnection? connection = null;
        bool dispose = false;
        try
        {
            if (useTransaction)
            {
                if (_transaction == null || _transactionConnection == null)
                {
                    throw new DbaTransactionException("Transaction has not been started.");
                }
                connection = _transactionConnection;
            }
            else
            {
                connection = new OracleConnection(connectionString);
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                dispose = true;
            }
            using var command = connection.CreateCommand();
            command.CommandText = procedure;
            command.CommandType = CommandType.StoredProcedure;
            command.Transaction = useTransaction ? _transaction : null;
            AddParameters(command, parameters);
            var commandTimeout = CommandTimeout;
            if (commandTimeout > 0)
            {
                command.CommandTimeout = commandTimeout;
            }
            var dataSet = new DataSet();
            using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            var tableIndex = 0;
            do
            {
                var table = new DataTable($"Table{tableIndex}");
                table.Load(reader);
                dataSet.Tables.Add(table);
                tableIndex++;
            } while (!reader.IsClosed && await reader.NextResultAsync(cancellationToken).ConfigureAwait(false));
            return BuildResult(dataSet);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute stored procedure.", procedure, ex);
        }
        finally
        {
            if (dispose)
            {
                connection?.Dispose();
            }
        }
    }

    /// <summary>
    /// Performs a bulk insert into an Oracle table using <see cref="OracleBulkCopy"/>.
    /// </summary>
    /// <param name="host">Host name or IP address of the Oracle listener.</param>
    /// <param name="serviceName">Oracle service name or SID.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="table">Source data to insert.</param>
    /// <param name="destinationTable">Fully qualified destination table name.</param>
    /// <param name="useTransaction"><see langword="true"/> to reuse the ambient transaction started by <see cref="BeginTransaction(string, string, string, string)"/>.</param>
    /// <param name="batchSize">Optional batch size used to split uploads into chunks.</param>
    /// <param name="bulkCopyTimeout">Optional command timeout in seconds for the bulk copy operation.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="table"/> is <see langword="null"/>.</exception>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction is active.</exception>
    /// <exception cref="DbaQueryExecutionException">Wraps any Oracle exception encountered during bulk copy.</exception>
    public virtual void BulkInsert(string host, string serviceName, string username, string password, DataTable table, string destinationTable, bool useTransaction = false, int? batchSize = null, int? bulkCopyTimeout = null)
    {
        if (table == null) throw new ArgumentNullException(nameof(table));

        var connectionString = BuildConnectionString(host, serviceName, username, password);

        OracleConnection? connection = null;
        bool dispose = false;
        if (useTransaction)
        {
            if (_transaction == null || _transactionConnection == null)
            {
                throw new DbaTransactionException("Transaction has not been started.");
            }
            connection = _transactionConnection;
        }
        else
        {
            connection = CreateConnection(connectionString);
            OpenConnection(connection);
            dispose = true;
        }
        try
        {
            using var bulkCopy = CreateBulkCopy(connection, useTransaction ? _transaction : null);
            bulkCopy.DestinationTableName = destinationTable;
            if (bulkCopyTimeout.HasValue)
            {
                bulkCopy.BulkCopyTimeout = bulkCopyTimeout.Value;
            }

            foreach (DataColumn column in table.Columns)
            {
                bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
            }

            if (batchSize.HasValue && batchSize.Value > 0)
            {
                var totalRows = table.Rows.Count;
                for (int offset = 0; offset < totalRows; offset += batchSize.Value)
                {
                    var batchTable = table.Clone();
                    for (int i = offset; i < Math.Min(offset + batchSize.Value, totalRows); i++)
                    {
                        batchTable.ImportRow(table.Rows[i]);
                    }
                    WriteToServer(bulkCopy, batchTable);
                }
            }
            else
            {
                WriteToServer(bulkCopy, table);
            }
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute bulk insert.", destinationTable, ex);
        }
        finally
        {
            if (dispose)
            {
                connection?.Dispose();
            }
        }
    }

    /// <summary>
    /// Asynchronously performs a bulk insert into an Oracle table using <see cref="OracleBulkCopy"/>.
    /// </summary>
    /// <param name="host">Host name or IP address of the Oracle listener.</param>
    /// <param name="serviceName">Oracle service name or SID.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="table">Source data to insert.</param>
    /// <param name="destinationTable">Fully qualified destination table name.</param>
    /// <param name="useTransaction"><see langword="true"/> to reuse the ambient transaction started by <see cref="BeginTransaction(string, string, string, string)"/>.</param>
    /// <param name="batchSize">Optional batch size used to split uploads into chunks.</param>
    /// <param name="bulkCopyTimeout">Optional command timeout in seconds for the bulk copy operation.</param>
    /// <param name="cancellationToken">Token used to cancel the bulk copy operation.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="table"/> is <see langword="null"/>.</exception>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction is active.</exception>
    /// <exception cref="DbaQueryExecutionException">Wraps any Oracle exception encountered during bulk copy.</exception>
    public virtual async Task BulkInsertAsync(string host, string serviceName, string username, string password, DataTable table, string destinationTable, bool useTransaction = false, int? batchSize = null, int? bulkCopyTimeout = null, CancellationToken cancellationToken = default)
    {
        if (table == null) throw new ArgumentNullException(nameof(table));

        var connectionString = BuildConnectionString(host, serviceName, username, password);

        OracleConnection? connection = null;
        bool dispose = false;
        if (useTransaction)
        {
            if (_transaction == null || _transactionConnection == null)
            {
                throw new DbaTransactionException("Transaction has not been started.");
            }
            connection = _transactionConnection;
        }
        else
        {
            connection = CreateConnection(connectionString);
            await OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);
            dispose = true;
        }
        try
        {
            using var bulkCopy = CreateBulkCopy(connection, useTransaction ? _transaction : null);
            bulkCopy.DestinationTableName = destinationTable;
            if (bulkCopyTimeout.HasValue)
            {
                bulkCopy.BulkCopyTimeout = bulkCopyTimeout.Value;
            }

            foreach (DataColumn column in table.Columns)
            {
                bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
            }

            if (batchSize.HasValue && batchSize.Value > 0)
            {
                var totalRows = table.Rows.Count;
                for (int offset = 0; offset < totalRows; offset += batchSize.Value)
                {
                    var batchTable = table.Clone();
                    for (int i = offset; i < Math.Min(offset + batchSize.Value, totalRows); i++)
                    {
                        batchTable.ImportRow(table.Rows[i]);
                    }
                    await WriteToServerAsync(bulkCopy, batchTable, cancellationToken).ConfigureAwait(false);
                }
            }
            else
            {
                await WriteToServerAsync(bulkCopy, table, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute bulk insert.", destinationTable, ex);
        }
        finally
        {
            if (dispose)
            {
                connection?.Dispose();
            }
        }
    }

    /// <summary>
    /// Creates the <see cref="OracleBulkCopy"/> instance used by the bulk insert helpers.
    /// </summary>
    /// <param name="connection">Open Oracle connection.</param>
    /// <param name="transaction">Optional ambient transaction.</param>
    /// <returns>The bulk copy instance to use for the operation.</returns>
    /// <remarks>Override to configure provider-specific options such as array binding or internal buffering strategies.</remarks>
    protected virtual OracleBulkCopy CreateBulkCopy(OracleConnection connection, OracleTransaction? transaction) => new(connection);

    /// <summary>
    /// Performs the synchronous write for the supplied <see cref="OracleBulkCopy"/>.
    /// </summary>
    /// <param name="bulkCopy">Bulk copy instance.</param>
    /// <param name="table">Source table data.</param>
    /// <remarks>Override to customize batching or to hook telemetry.</remarks>
    protected virtual void WriteToServer(OracleBulkCopy bulkCopy, DataTable table) => bulkCopy.WriteToServer(table);

    /// <summary>
    /// Performs the asynchronous write for the supplied <see cref="OracleBulkCopy"/>.
    /// </summary>
    /// <param name="bulkCopy">Bulk copy instance.</param>
    /// <param name="table">Source table data.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous copy.</returns>
    /// <remarks>The default implementation delegates to the synchronous overload to preserve backwards compatibility.</remarks>
    protected virtual Task WriteToServerAsync(OracleBulkCopy bulkCopy, DataTable table, CancellationToken cancellationToken)
    {
        WriteToServer(bulkCopy, table);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Creates an <see cref="OracleConnection"/> for the provided connection string.
    /// </summary>
    /// <param name="connectionString">Connection string generated by <see cref="BuildConnectionString(string, string, string, string, int?)"/> or provided externally.</param>
    /// <returns>An unopened Oracle connection.</returns>
    /// <remarks>Override to plug connection pooling or diagnostics scenarios.</remarks>
    protected virtual OracleConnection CreateConnection(string connectionString) => new(connectionString);

    /// <summary>
    /// Opens the supplied <see cref="OracleConnection"/> synchronously.
    /// </summary>
    /// <param name="connection">Connection to open.</param>
    /// <remarks>Override to add logging or to handle provider-specific retries.</remarks>
    protected virtual void OpenConnection(OracleConnection connection) => connection.Open();

    /// <summary>
    /// Opens the supplied <see cref="OracleConnection"/> asynchronously.
    /// </summary>
    /// <param name="connection">Connection to open.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous open.</returns>
    /// <remarks>Override to add logging or to handle provider-specific retries.</remarks>
    protected virtual Task OpenConnectionAsync(OracleConnection connection, CancellationToken cancellationToken) => connection.OpenAsync(cancellationToken);

    /// <summary>
    /// Begins a transaction using the Oracle connection derived from the provided credentials.
    /// </summary>
    /// <param name="host">Host name or IP address of the Oracle listener.</param>
    /// <param name="serviceName">Oracle service name or SID.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <exception cref="DbaTransactionException">Thrown when a transaction is already active.</exception>
    public virtual void BeginTransaction(string host, string serviceName, string username, string password) =>
        BeginTransaction(host, serviceName, username, password, IsolationLevel.ReadCommitted);

    /// <summary>
    /// Begins a transaction using the Oracle connection derived from the provided credentials and isolation level.
    /// </summary>
    /// <param name="host">Host name or IP address of the Oracle listener.</param>
    /// <param name="serviceName">Oracle service name or SID.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="isolationLevel">Transaction isolation level.</param>
    /// <exception cref="DbaTransactionException">Thrown when a transaction is already active.</exception>
    public virtual void BeginTransaction(string host, string serviceName, string username, string password, IsolationLevel isolationLevel)
    {
        lock (_syncRoot)
        {
            if (_transaction != null)
            {
                throw new DbaTransactionException("Transaction already started.");
            }
            var connectionString = BuildConnectionString(host, serviceName, username, password);
            _transactionConnection = new OracleConnection(connectionString);
            _transactionConnection.Open();
            _transaction = _transactionConnection.BeginTransaction(isolationLevel);
        }
    }

    /// <summary>
    /// Asynchronously begins a transaction using the Oracle connection derived from the provided credentials.
    /// </summary>
    /// <param name="host">Host name or IP address of the Oracle listener.</param>
    /// <param name="serviceName">Oracle service name or SID.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <exception cref="DbaTransactionException">Thrown when a transaction is already active.</exception>
    public virtual Task BeginTransactionAsync(string host, string serviceName, string username, string password, CancellationToken cancellationToken = default) =>
        BeginTransactionAsync(host, serviceName, username, password, IsolationLevel.ReadCommitted, cancellationToken);

    /// <summary>
    /// Asynchronously begins a transaction using the Oracle connection derived from the provided credentials and isolation level.
    /// </summary>
    /// <param name="host">Host name or IP address of the Oracle listener.</param>
    /// <param name="serviceName">Oracle service name or SID.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="isolationLevel">Transaction isolation level.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <exception cref="DbaTransactionException">Thrown when a transaction is already active.</exception>
    public virtual async Task BeginTransactionAsync(string host, string serviceName, string username, string password, IsolationLevel isolationLevel, CancellationToken cancellationToken = default)
    {
        lock (_syncRoot)
        {
            if (_transaction != null)
            {
                throw new DbaTransactionException("Transaction already started.");
            }
        }
        var connectionString = BuildConnectionString(host, serviceName, username, password);
        var connection = new OracleConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
        var transaction = (OracleTransaction)await connection.BeginTransactionAsync(isolationLevel, cancellationToken).ConfigureAwait(false);
#else
        var transaction = connection.BeginTransaction(isolationLevel);
#endif
        lock (_syncRoot)
        {
            if (_transaction != null)
            {
                transaction.Dispose();
                connection.Dispose();
                throw new DbaTransactionException("Transaction already started.");
            }
            _transactionConnection = connection;
            _transaction = transaction;
        }
    }

    /// <summary>
    /// Commits the active transaction.
    /// </summary>
    /// <exception cref="DbaTransactionException">Thrown when no transaction is active.</exception>
    public virtual void Commit()
    {
        lock (_syncRoot)
        {
            if (_transaction == null)
            {
                throw new DbaTransactionException("No active transaction.");
            }
            _transaction.Commit();
            DisposeTransactionLocked();
        }
    }

    /// <summary>
    /// Asynchronously commits the active transaction.
    /// </summary>
    /// <param name="cancellationToken">Token used to cancel the commit operation.</param>
    /// <exception cref="DbaTransactionException">Thrown when no transaction is active.</exception>
    public virtual async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        OracleTransaction? tx;
        OracleConnection? conn;
        lock (_syncRoot)
        {
            if (_transaction == null)
            {
                throw new DbaTransactionException("No active transaction.");
            }
            tx = _transaction;
            conn = _transactionConnection;
            _transaction = null;
            _transactionConnection = null;
        }
        try
        {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
            await tx!.CommitAsync(cancellationToken).ConfigureAwait(false);
#else
            tx!.Commit();
#endif
        }
        finally
        {
            tx!.Dispose();
            conn?.Dispose();
        }
    }

    /// <summary>
    /// Rolls back the active transaction.
    /// </summary>
    /// <exception cref="DbaTransactionException">Thrown when no transaction is active.</exception>
    public virtual void Rollback()
    {
        lock (_syncRoot)
        {
            if (_transaction == null)
            {
                throw new DbaTransactionException("No active transaction.");
            }
            _transaction.Rollback();
            DisposeTransactionLocked();
        }
    }

    /// <summary>
    /// Asynchronously rolls back the active transaction.
    /// </summary>
    /// <param name="cancellationToken">Token used to cancel the rollback operation.</param>
    /// <exception cref="DbaTransactionException">Thrown when no transaction is active.</exception>
    public virtual async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        OracleTransaction? tx;
        OracleConnection? conn;
        lock (_syncRoot)
        {
            if (_transaction == null)
            {
                throw new DbaTransactionException("No active transaction.");
            }
            tx = _transaction;
            conn = _transactionConnection;
            _transaction = null;
            _transactionConnection = null;
        }
        try
        {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
            await tx!.RollbackAsync(cancellationToken).ConfigureAwait(false);
#else
            tx!.Rollback();
#endif
        }
        finally
        {
            tx!.Dispose();
            conn?.Dispose();
        }
    }

    private void DisposeTransaction()
    {
        lock (_syncRoot)
        {
            DisposeTransactionLocked();
        }
    }

    private void DisposeTransactionLocked()
    {
        _transaction?.Dispose();
        _transaction = null;
        _transactionConnection?.Dispose();
        _transactionConnection = null;
    }

    /// <summary>
    /// Determines whether the specified exception represents a transient Oracle connectivity failure.
    /// </summary>
    /// <param name="ex">Exception to evaluate.</param>
    /// <returns><see langword="true"/> when the exception number maps to a retryable Oracle error.</returns>
    protected override bool IsTransient(Exception ex) =>
        ex is OracleException oex && (oex.Number == 12541 || oex.Number == 12545 || oex.Number == 1089 || oex.Number == 3113 || oex.Number == 3114);

    /// <summary>
    /// Disposes the client and releases any held transaction resources.
    /// </summary>
    /// <param name="disposing">Indicates whether the method is invoked from <see cref="DatabaseClientBase.Dispose()"/>.</param>
    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            DisposeTransaction();
        }
        base.Dispose(disposing);
    }
}
