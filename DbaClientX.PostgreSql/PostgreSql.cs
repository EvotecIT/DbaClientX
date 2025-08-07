using System;
using System.Data;
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using Npgsql;
using NpgsqlTypes;
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
using System.Runtime.CompilerServices;
#endif

namespace DBAClientX;

/// <summary>
/// This class is used to connect to PostgreSQL
/// </summary>
public class PostgreSql : DatabaseClientBase
{
    private readonly object _syncRoot = new();
    private NpgsqlConnection? _transactionConnection;
    private NpgsqlTransaction? _transaction;

    public bool IsInTransaction => _transaction != null;

    public static string BuildConnectionString(string host, string database, string username, string password)
    {
        return new NpgsqlConnectionStringBuilder
        {
            Host = host,
            Database = database,
            Username = username,
            Password = password,
            Pooling = true
        }.ConnectionString;
    }

    public virtual bool Ping(string host, string database, string username, string password, string? connectionString = null)
    {
        try
        {
            Query(host, database, username, password, "SELECT 1", connectionString: connectionString);
            return true;
        }
        catch
        {
            return false;
        }
    }

    public virtual async Task<bool> PingAsync(string host, string database, string username, string password, CancellationToken cancellationToken = default, string? connectionString = null)
    {
        try
        {
            await QueryAsync(host, database, username, password, "SELECT 1", cancellationToken: cancellationToken, connectionString: connectionString).ConfigureAwait(false);
            return true;
        }
        catch
        {
            return false;
        }
    }

    public virtual object? Query(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, NpgsqlDbType>? parameterTypes = null, string? connectionString = null)
    {
        var cs = connectionString ?? BuildConnectionString(host, database, username, password);

        NpgsqlConnection? connection = null;
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
                connection = new NpgsqlConnection(cs);
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

    private static IDictionary<string, DbType>? ConvertParameterTypes(IDictionary<string, NpgsqlDbType>? types) =>
        DbTypeConverter.ConvertParameterTypes(types, static () => new NpgsqlParameter(), static (p, t) => p.NpgsqlDbType = t);

    public virtual int ExecuteNonQuery(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, NpgsqlDbType>? parameterTypes = null, string? connectionString = null)
    {
        var cs = connectionString ?? BuildConnectionString(host, database, username, password);

        NpgsqlConnection? connection = null;
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
                connection = new NpgsqlConnection(cs);
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

    public virtual async Task<object?> QueryAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, NpgsqlDbType>? parameterTypes = null, string? connectionString = null)
    {
        var cs = connectionString ?? BuildConnectionString(host, database, username, password);

        NpgsqlConnection? connection = null;
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
                connection = new NpgsqlConnection(cs);
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
            return $"CALL {procedure}()";
        }
        var joined = string.Join(", ", parameters.Keys);
        return $"CALL {procedure}({joined})";
    }

    public virtual object? ExecuteStoredProcedure(string host, string database, string username, string password, string procedure, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, NpgsqlDbType>? parameterTypes = null)
    {
        var query = BuildStoredProcedureQuery(procedure, parameters);
        return Query(host, database, username, password, query, parameters, useTransaction, parameterTypes);
    }

    public virtual Task<object?> ExecuteStoredProcedureAsync(string host, string database, string username, string password, string procedure, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, NpgsqlDbType>? parameterTypes = null)
    {
        var query = BuildStoredProcedureQuery(procedure, parameters);
        return QueryAsync(host, database, username, password, query, parameters, useTransaction, cancellationToken, parameterTypes);
    }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
    public virtual IAsyncEnumerable<DataRow> QueryStreamAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, [EnumeratorCancellation] CancellationToken cancellationToken = default, IDictionary<string, NpgsqlDbType>? parameterTypes = null, string? connectionString = null)
    {
        return Stream();

        async IAsyncEnumerable<DataRow> Stream()
        {
            var cs = connectionString ?? BuildConnectionString(host, database, username, password);

            NpgsqlConnection? connection = null;
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
                connection = new NpgsqlConnection(cs);
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

    public virtual void BeginTransaction(string host, string database, string username, string password, string? connectionString = null)
    {
        lock (_syncRoot)
        {
            if (_transaction != null)
            {
                throw new DbaTransactionException("Transaction already started.");
            }

            var cs = connectionString ?? BuildConnectionString(host, database, username, password);

            _transactionConnection = new NpgsqlConnection(cs);
            _transactionConnection.Open();
            _transaction = _transactionConnection.BeginTransaction();
        }
    }

    public virtual async Task BeginTransactionAsync(string host, string database, string username, string password, CancellationToken cancellationToken = default, string? connectionString = null)
    {
        if (_transaction != null)
        {
            throw new DbaTransactionException("Transaction already started.");
        }

        var cs = connectionString ?? BuildConnectionString(host, database, username, password);

        _transactionConnection = new NpgsqlConnection(cs);
        await _transactionConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
        _transaction = await _transactionConnection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
#else
        _transaction = _transactionConnection.BeginTransaction();
#endif
    }

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

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            DisposeTransaction();
        }
        base.Dispose(disposing);
    }

    public async Task<IReadOnlyList<object?>> RunQueriesInParallel(IEnumerable<string> queries, string host, string database, string username, string password, CancellationToken cancellationToken = default, string? connectionString = null)
    {
        if (queries == null)
        {
            throw new ArgumentNullException(nameof(queries));
        }

        var tasks = queries.Select(q => QueryAsync(host, database, username, password, q, null, false, cancellationToken, connectionString: connectionString));
        var results = await Task.WhenAll(tasks).ConfigureAwait(false);
        return results;
    }
}
