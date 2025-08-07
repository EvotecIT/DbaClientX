using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
using System.Runtime.CompilerServices;
#endif

namespace DBAClientX;

/// <summary>
/// This class is used to connect to SQL Server
/// </summary>
public class SqlServer : DatabaseClientBase
{
    private readonly object _syncRoot = new();
    private SqlConnection? _transactionConnection;
    private SqlTransaction? _transaction;
    private static readonly ConcurrentDictionary<SqlDbType, DbType> TypeCache = new();

    public bool IsInTransaction => _transaction != null;

    /// <summary>
    /// Executes a query against a SQL Server database.
    /// </summary>
    /// <param name="serverOrInstance">The server or instance name.</param>
    /// <param name="database">The database name.</param>
    /// <param name="integratedSecurity">True to use Windows authentication.</param>
    /// <param name="query">The SQL query to execute.</param>
    /// <param name="parameters">Optional query parameters.</param>
    /// <param name="useTransaction">True to execute within an existing transaction.</param>
    /// <param name="parameterTypes">Optional parameter type mapping.</param>
    /// <param name="username">The SQL login name when not using integrated security.</param>
    /// <param name="password">The SQL login password when not using integrated security.</param>
    /// <returns>The query result, depending on <see cref="DatabaseClientBase.ReturnType"/>.</returns>
    public virtual object? Query(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, SqlDbType>? parameterTypes = null, string? username = null, string? password = null)
    {
        var connectionStringBuilder = new SqlConnectionStringBuilder
        {
            DataSource = serverOrInstance,
            InitialCatalog = database,
            IntegratedSecurity = integratedSecurity,
            Pooling = true
        };
        if (!integratedSecurity)
        {
            connectionStringBuilder.UserID = username;
            connectionStringBuilder.Password = password;
        }
        var connectionString = connectionStringBuilder.ConnectionString;

        SqlConnection? connection = null;
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
                connection = new SqlConnection(connectionString);
                connection.Open();
                dispose = true;
            }

            var dbTypes = ConvertParameterTypes(parameterTypes);
            return ExecuteQuery(connection, useTransaction ? _transaction : null, query, parameters, dbTypes);
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

    private static IDictionary<string, DbType>? ConvertParameterTypes(IDictionary<string, SqlDbType>? types)
    {
        if (types == null)
        {
            return null;
        }

        var result = new Dictionary<string, DbType>(types.Count);
        foreach (var pair in types)
        {
            var dbType = TypeCache.GetOrAdd(pair.Value, static s =>
            {
                var parameter = new SqlParameter { SqlDbType = s };
                return parameter.DbType;
            });
            result[pair.Key] = dbType;
        }
        return result;
    }

    /// <summary>
    /// Executes a non-query SQL command against a SQL Server database.
    /// </summary>
    /// <param name="serverOrInstance">The server or instance name.</param>
    /// <param name="database">The database name.</param>
    /// <param name="integratedSecurity">True to use Windows authentication.</param>
    /// <param name="query">The SQL command to execute.</param>
    /// <param name="parameters">Optional command parameters.</param>
    /// <param name="useTransaction">True to execute within an existing transaction.</param>
    /// <param name="parameterTypes">Optional parameter type mapping.</param>
    /// <param name="username">The SQL login name when not using integrated security.</param>
    /// <param name="password">The SQL login password when not using integrated security.</param>
    /// <returns>The number of affected rows.</returns>
    public virtual int ExecuteNonQuery(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, SqlDbType>? parameterTypes = null, string? username = null, string? password = null)
    {
        var connectionStringBuilder = new SqlConnectionStringBuilder
        {
            DataSource = serverOrInstance,
            InitialCatalog = database,
            IntegratedSecurity = integratedSecurity,
            Pooling = true
        };
        if (!integratedSecurity)
        {
            connectionStringBuilder.UserID = username;
            connectionStringBuilder.Password = password;
        }
        var connectionString = connectionStringBuilder.ConnectionString;

        SqlConnection? connection = null;
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
                connection = new SqlConnection(connectionString);
                connection.Open();
                dispose = true;
            }

            var dbTypes = ConvertParameterTypes(parameterTypes);
            return ExecuteNonQuery(connection, useTransaction ? _transaction : null, query, parameters, dbTypes);
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
    /// Asynchronously executes a query against a SQL Server database.
    /// </summary>
    /// <param name="serverOrInstance">The server or instance name.</param>
    /// <param name="database">The database name.</param>
    /// <param name="integratedSecurity">True to use Windows authentication.</param>
    /// <param name="query">The SQL query to execute.</param>
    /// <param name="parameters">Optional query parameters.</param>
    /// <param name="useTransaction">True to execute within an existing transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <param name="parameterTypes">Optional parameter type mapping.</param>
    /// <param name="username">The SQL login name when not using integrated security.</param>
    /// <param name="password">The SQL login password when not using integrated security.</param>
    /// <returns>A task representing the asynchronous operation with the query result.</returns>
    public virtual async Task<object?> QueryAsync(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, SqlDbType>? parameterTypes = null, string? username = null, string? password = null)
    {
        var connectionStringBuilder = new SqlConnectionStringBuilder
        {
            DataSource = serverOrInstance,
            InitialCatalog = database,
            IntegratedSecurity = integratedSecurity,
            Pooling = true
        };
        if (!integratedSecurity)
        {
            connectionStringBuilder.UserID = username;
            connectionStringBuilder.Password = password;
        }
        var connectionString = connectionStringBuilder.ConnectionString;

        SqlConnection? connection = null;
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
                connection = new SqlConnection(connectionString);
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                dispose = true;
            }

            var dbTypes = ConvertParameterTypes(parameterTypes);
            return await ExecuteQueryAsync(connection, useTransaction ? _transaction : null, query, parameters, cancellationToken, dbTypes).ConfigureAwait(false);
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

    private static string BuildStoredProcedureQuery(string procedure, IDictionary<string, object?>? parameters)
    {
        if (parameters == null || parameters.Count == 0)
        {
            return $"EXEC {procedure}";
        }
        var joined = string.Join(", ", parameters.Keys);
        return $"EXEC {procedure} {joined}";
    }

    /// <summary>
    /// Executes a stored procedure and returns the result.
    /// </summary>
    /// <param name="serverOrInstance">The server or instance name.</param>
    /// <param name="database">The database name.</param>
    /// <param name="integratedSecurity">True to use Windows authentication.</param>
    /// <param name="procedure">The stored procedure name.</param>
    /// <param name="parameters">Optional procedure parameters.</param>
    /// <param name="useTransaction">True to execute within an existing transaction.</param>
    /// <param name="parameterTypes">Optional parameter type mapping.</param>
    /// <param name="username">The SQL login name when not using integrated security.</param>
    /// <param name="password">The SQL login password when not using integrated security.</param>
    /// <returns>The result of the stored procedure.</returns>
    public virtual object? ExecuteStoredProcedure(string serverOrInstance, string database, bool integratedSecurity, string procedure, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, SqlDbType>? parameterTypes = null, string? username = null, string? password = null)
    {
        var query = BuildStoredProcedureQuery(procedure, parameters);
        return Query(serverOrInstance, database, integratedSecurity, query, parameters, useTransaction, parameterTypes, username, password);
    }

    /// <summary>
    /// Asynchronously executes a stored procedure and returns the result.
    /// </summary>
    /// <param name="serverOrInstance">The server or instance name.</param>
    /// <param name="database">The database name.</param>
    /// <param name="integratedSecurity">True to use Windows authentication.</param>
    /// <param name="procedure">The stored procedure name.</param>
    /// <param name="parameters">Optional procedure parameters.</param>
    /// <param name="useTransaction">True to execute within an existing transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <param name="parameterTypes">Optional parameter type mapping.</param>
    /// <param name="username">The SQL login name when not using integrated security.</param>
    /// <param name="password">The SQL login password when not using integrated security.</param>
    /// <returns>A task representing the asynchronous operation with the procedure result.</returns>
    public virtual Task<object?> ExecuteStoredProcedureAsync(string serverOrInstance, string database, bool integratedSecurity, string procedure, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, SqlDbType>? parameterTypes = null, string? username = null, string? password = null)
    {
        var query = BuildStoredProcedureQuery(procedure, parameters);
        return QueryAsync(serverOrInstance, database, integratedSecurity, query, parameters, useTransaction, cancellationToken, parameterTypes, username, password);
    }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
    /// <summary>
    /// Asynchronously streams query results from a SQL Server database.
    /// </summary>
    /// <param name="serverOrInstance">The server or instance name.</param>
    /// <param name="database">The database name.</param>
    /// <param name="integratedSecurity">True to use Windows authentication.</param>
    /// <param name="query">The SQL query to execute.</param>
    /// <param name="parameters">Optional query parameters.</param>
    /// <param name="useTransaction">True to execute within an existing transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <param name="parameterTypes">Optional parameter type mapping.</param>
    /// <param name="username">The SQL login name when not using integrated security.</param>
    /// <param name="password">The SQL login password when not using integrated security.</param>
    /// <returns>An asynchronous stream of <see cref="DataRow"/> objects.</returns>
    public virtual IAsyncEnumerable<DataRow> QueryStreamAsync(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, [EnumeratorCancellation] CancellationToken cancellationToken = default, IDictionary<string, SqlDbType>? parameterTypes = null, string? username = null, string? password = null)
    {
        return Stream();

        async IAsyncEnumerable<DataRow> Stream()
        {
            var connectionStringBuilder = new SqlConnectionStringBuilder
            {
                DataSource = serverOrInstance,
                InitialCatalog = database,
                IntegratedSecurity = integratedSecurity,
                Pooling = true
            };
            if (!integratedSecurity)
            {
                connectionStringBuilder.UserID = username;
                connectionStringBuilder.Password = password;
            }
            var connectionString = connectionStringBuilder.ConnectionString;

            SqlConnection? connection = null;
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
                connection = new SqlConnection(connectionString);
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                dispose = true;
            }

            var dbTypes = ConvertParameterTypes(parameterTypes);
            try
            {
                await foreach (var row in ExecuteQueryStreamAsync(connection, useTransaction ? _transaction : null, query, parameters, cancellationToken, dbTypes).ConfigureAwait(false))
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
    /// Begins a database transaction.
    /// </summary>
    /// <param name="serverOrInstance">The server or instance name.</param>
    /// <param name="database">The database name.</param>
    /// <param name="integratedSecurity">True to use Windows authentication.</param>
    /// <param name="username">The SQL login name when not using integrated security.</param>
    /// <param name="password">The SQL login password when not using integrated security.</param>
    public virtual void BeginTransaction(string serverOrInstance, string database, bool integratedSecurity, string? username = null, string? password = null)
    {
        lock (_syncRoot)
        {
            if (_transaction != null)
            {
                throw new DbaTransactionException("Transaction already started.");
            }

            var connectionStringBuilder = new SqlConnectionStringBuilder
            {
                DataSource = serverOrInstance,
                InitialCatalog = database,
                IntegratedSecurity = integratedSecurity,
                Pooling = true
            };
            if (!integratedSecurity)
            {
                connectionStringBuilder.UserID = username;
                connectionStringBuilder.Password = password;
            }
            var connectionString = connectionStringBuilder.ConnectionString;

            _transactionConnection = new SqlConnection(connectionString);
            _transactionConnection.Open();
            _transaction = _transactionConnection.BeginTransaction();
        }
    }

    /// <summary>
    /// Asynchronously begins a database transaction.
    /// </summary>
    /// <param name="serverOrInstance">The server or instance name.</param>
    /// <param name="database">The database name.</param>
    /// <param name="integratedSecurity">True to use Windows authentication.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <param name="username">The SQL login name when not using integrated security.</param>
    /// <param name="password">The SQL login password when not using integrated security.</param>
    public virtual async Task BeginTransactionAsync(string serverOrInstance, string database, bool integratedSecurity, CancellationToken cancellationToken = default, string? username = null, string? password = null)
    {
        if (_transaction != null)
        {
            throw new DbaTransactionException("Transaction already started.");
        }

        var connectionStringBuilder = new SqlConnectionStringBuilder
        {
            DataSource = serverOrInstance,
            InitialCatalog = database,
            IntegratedSecurity = integratedSecurity,
            Pooling = true
        };
        if (!integratedSecurity)
        {
            connectionStringBuilder.UserID = username;
            connectionStringBuilder.Password = password;
        }
        var connectionString = connectionStringBuilder.ConnectionString;

        _transactionConnection = new SqlConnection(connectionString);
        await _transactionConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
        _transaction = (SqlTransaction)await _transactionConnection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
#else
        _transaction = _transactionConnection.BeginTransaction();
#endif
    }

    /// <summary>
    /// Commits the current transaction.
    /// </summary>
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
    /// Asynchronously commits the current transaction.
    /// </summary>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    public virtual async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        if (_transaction == null)
        {
            throw new DbaTransactionException("No active transaction.");
        }
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
        await _transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
#else
        _transaction.Commit();
#endif
        DisposeTransaction();
    }

    /// <summary>
    /// Rolls back the current transaction.
    /// </summary>
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
    /// Asynchronously rolls back the current transaction.
    /// </summary>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    public virtual async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        if (_transaction == null)
        {
            throw new DbaTransactionException("No active transaction.");
        }
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
        await _transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
#else
        _transaction.Rollback();
#endif
        DisposeTransaction();
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
    /// Executes multiple queries in parallel.
    /// </summary>
    /// <param name="queries">The collection of queries to execute.</param>
    /// <param name="serverOrInstance">The server or instance name.</param>
    /// <param name="database">The database name.</param>
    /// <param name="integratedSecurity">True to use Windows authentication.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <param name="username">The SQL login name when not using integrated security.</param>
    /// <param name="password">The SQL login password when not using integrated security.</param>
    /// <returns>A list of results for each executed query.</returns>
    public async Task<IReadOnlyList<object?>> RunQueriesInParallel(IEnumerable<string> queries, string serverOrInstance, string database, bool integratedSecurity, CancellationToken cancellationToken = default, string? username = null, string? password = null)
    {
        if (queries == null)
        {
            throw new ArgumentNullException(nameof(queries));
        }

        var tasks = queries.Select(q => QueryAsync(serverOrInstance, database, integratedSecurity, q, null, false, cancellationToken, username: username, password: password));
        var results = await Task.WhenAll(tasks).ConfigureAwait(false);
        return results;
    }
}