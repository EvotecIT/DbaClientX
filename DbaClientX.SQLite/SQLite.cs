using System;
using System.Data;
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using Microsoft.Data.Sqlite;
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
using System.Runtime.CompilerServices;
#endif

namespace DBAClientX;

/// <summary>
/// This class is used to connect to SQLite
/// </summary>
public class SQLite : DatabaseClientBase
{
    private readonly object _syncRoot = new();
    private SqliteConnection? _transactionConnection;
    private SqliteTransaction? _transaction;
    private static readonly ConcurrentDictionary<SqliteType, DbType> TypeCache = new();

    public bool IsInTransaction => _transaction != null;

    /// <summary>
    /// Executes a query against a SQLite database file.
    /// </summary>
    /// <param name="database">Path to the SQLite database file.</param>
    /// <param name="query">The SQL query to execute.</param>
    /// <param name="parameters">Optional query parameters.</param>
    /// <param name="useTransaction">True to execute within an existing transaction.</param>
    /// <param name="parameterTypes">Optional parameter type mapping.</param>
    /// <returns>The query result, depending on <see cref="DatabaseClientBase.ReturnType"/>.</returns>
    public virtual object? Query(string database, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, SqliteType>? parameterTypes = null)
    {
        var connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = database,
            Pooling = true
        }.ConnectionString;

        SqliteConnection? connection = null;
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
                connection = new SqliteConnection(connectionString);
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

    private static IDictionary<string, DbType>? ConvertParameterTypes(IDictionary<string, SqliteType>? types)
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
                var parameter = new SqliteParameter { SqliteType = s };
                return parameter.DbType;
            });
            result[pair.Key] = dbType;
        }
        return result;
    }

    /// <summary>
    /// Executes a non-query SQL command against a SQLite database.
    /// </summary>
    /// <param name="database">Path to the SQLite database file.</param>
    /// <param name="query">The SQL command to execute.</param>
    /// <param name="parameters">Optional command parameters.</param>
    /// <param name="useTransaction">True to execute within an existing transaction.</param>
    /// <param name="parameterTypes">Optional parameter type mapping.</param>
    /// <returns>The number of affected rows.</returns>
    public virtual int ExecuteNonQuery(string database, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, SqliteType>? parameterTypes = null)
    {
        var connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = database,
            Pooling = true
        }.ConnectionString;

        SqliteConnection? connection = null;
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
                connection = new SqliteConnection(connectionString);
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
    /// Asynchronously executes a query against a SQLite database file.
    /// </summary>
    /// <param name="database">Path to the SQLite database file.</param>
    /// <param name="query">The SQL query to execute.</param>
    /// <param name="parameters">Optional query parameters.</param>
    /// <param name="useTransaction">True to execute within an existing transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <param name="parameterTypes">Optional parameter type mapping.</param>
    /// <returns>A task representing the asynchronous operation with the query result.</returns>
    public virtual async Task<object?> QueryAsync(string database, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, SqliteType>? parameterTypes = null)
    {
        var connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = database,
            Pooling = true
        }.ConnectionString;

        SqliteConnection? connection = null;
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
                connection = new SqliteConnection(connectionString);
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

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
    /// <summary>
    /// Asynchronously streams query results from a SQLite database file.
    /// </summary>
    /// <param name="database">Path to the SQLite database file.</param>
    /// <param name="query">The SQL query to execute.</param>
    /// <param name="parameters">Optional query parameters.</param>
    /// <param name="useTransaction">True to execute within an existing transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <param name="parameterTypes">Optional parameter type mapping.</param>
    /// <returns>An asynchronous stream of <see cref="DataRow"/> objects.</returns>
    public virtual async IAsyncEnumerable<DataRow> QueryStreamAsync(string database, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, [EnumeratorCancellation] CancellationToken cancellationToken = default, IDictionary<string, SqliteType>? parameterTypes = null)
    {
        var connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = database,
            Pooling = true
        }.ConnectionString;

        SqliteConnection? connection = null;
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
            connection = new SqliteConnection(connectionString);
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
                await connection.DisposeAsync().ConfigureAwait(false);
            }
        }
    }
#endif

    /// <summary>
    /// Begins a database transaction.
    /// </summary>
    /// <param name="database">Path to the SQLite database file.</param>
    public virtual void BeginTransaction(string database)
    {
        lock (_syncRoot)
        {
            if (_transaction != null)
            {
                throw new DbaTransactionException("Transaction already started.");
            }

            var connectionString = new SqliteConnectionStringBuilder
            {
                DataSource = database,
                Pooling = true
            }.ConnectionString;

            _transactionConnection = new SqliteConnection(connectionString);
            _transactionConnection.Open();
            _transaction = _transactionConnection.BeginTransaction();
        }
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
    /// <param name="database">Path to the SQLite database file.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>A list of results for each executed query.</returns>
    public async Task<IReadOnlyList<object?>> RunQueriesInParallel(IEnumerable<string> queries, string database, CancellationToken cancellationToken = default)
    {
        if (queries == null)
        {
            throw new ArgumentNullException(nameof(queries));
        }

        var tasks = queries.Select(q => QueryAsync(database, q, null, false, cancellationToken));
        var results = await Task.WhenAll(tasks).ConfigureAwait(false);
        return results;
    }
}
