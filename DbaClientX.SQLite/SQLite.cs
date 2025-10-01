using System;
using System.Data;
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Text;
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

    public bool IsInTransaction => _transaction != null;

    public static string BuildConnectionString(string database)
    {
        return new SqliteConnectionStringBuilder
        {
            DataSource = database,
            Pooling = true
        }.ConnectionString;
    }

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

    public virtual object? Query(string database, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, SqliteType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(database);

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

    public virtual object? ExecuteScalar(string database, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, SqliteType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(database);

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

    private static IDictionary<string, DbType>? ConvertParameterTypes(IDictionary<string, SqliteType>? types) =>
        DbTypeConverter.ConvertParameterTypes(types, static () => new SqliteParameter(), static (p, t) => p.SqliteType = t);

    public virtual int ExecuteNonQuery(
        string database,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, SqliteType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(database);

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

    public virtual async Task<int> ExecuteNonQueryAsync(string database, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, SqliteType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(database);

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
            return await base.ExecuteNonQueryAsync(connection, useTransaction ? _transaction : null, query, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false);
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

    public virtual async Task<object?> QueryAsync(string database, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, SqliteType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(database);

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

    public virtual async Task<object?> ExecuteScalarAsync(string database, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, SqliteType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(database);

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
    public virtual async IAsyncEnumerable<DataRow> QueryStreamAsync(string database, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, [EnumeratorCancellation] CancellationToken cancellationToken = default, IDictionary<string, SqliteType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(database);

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
            await foreach (var row in ExecuteQueryStreamAsync(connection, useTransaction ? _transaction : null, query, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false))
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

    public virtual void BulkInsert(string database, DataTable table, string destinationTable, bool useTransaction = false, int? batchSize = null)
    {
        if (table == null) throw new ArgumentNullException(nameof(table));

        var connectionString = BuildConnectionString(database);

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
            connection.Open();
            dispose = true;
        }

        SqliteTransaction? transaction = null;
        if (!useTransaction)
        {
            transaction = connection.BeginTransaction();
        }

        try
        {
            var totalRows = table.Rows.Count;
            var columns = string.Join(", ", table.Columns.Cast<DataColumn>().Select(c => $"\"{c.ColumnName}\""));
            var rowsPerBatch = batchSize.HasValue && batchSize.Value > 0 ? batchSize.Value : totalRows;

            for (int offset = 0; offset < totalRows; offset += rowsPerBatch)
            {
                var sb = new StringBuilder();
                sb.Append($"INSERT INTO {destinationTable} ({columns}) VALUES ");
                var parameters = new Dictionary<string, object?>();
                int paramIndex = 0;
                int max = Math.Min(offset + rowsPerBatch, totalRows);
                for (int i = offset; i < max; i++)
                {
                    if (i > offset) sb.Append(", ");
                    sb.Append("(");
                    int colIndex = 0;
                    foreach (DataColumn column in table.Columns)
                    {
                        var paramName = $"@p{paramIndex++}";
                        parameters[paramName] = table.Rows[i][column] ?? DBNull.Value;
                        if (colIndex > 0) sb.Append(", ");
                        sb.Append(paramName);
                        colIndex++;
                    }
                    sb.Append(")");
                }
                sb.Append(";");

                ExecuteNonQuery(connection, useTransaction ? _transaction : transaction, sb.ToString(), parameters);
            }

            if (!useTransaction)
            {
                transaction?.Commit();
            }
        }
        catch (Exception ex)
        {
            if (!useTransaction)
            {
                transaction?.Rollback();
            }
            throw new DbaQueryExecutionException("Failed to execute bulk insert.", destinationTable, ex);
        }
        finally
        {
            if (!useTransaction)
            {
                transaction?.Dispose();
            }
            if (dispose)
            {
                connection?.Dispose();
            }
        }
    }

    public virtual async Task BulkInsertAsync(string database, DataTable table, string destinationTable, bool useTransaction = false, int? batchSize = null, CancellationToken cancellationToken = default)
    {
        if (table == null) throw new ArgumentNullException(nameof(table));

        var connectionString = BuildConnectionString(database);

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

        SqliteTransaction? transaction = null;
        if (!useTransaction)
        {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
            transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
#else
            transaction = connection.BeginTransaction();
#endif
        }

        try
        {
            var totalRows = table.Rows.Count;
            var columns = string.Join(", ", table.Columns.Cast<DataColumn>().Select(c => $"\"{c.ColumnName}\""));
            var rowsPerBatch = batchSize.HasValue && batchSize.Value > 0 ? batchSize.Value : totalRows;

            for (int offset = 0; offset < totalRows; offset += rowsPerBatch)
            {
                var sb = new StringBuilder();
                sb.Append($"INSERT INTO {destinationTable} ({columns}) VALUES ");
                var parameters = new Dictionary<string, object?>();
                int paramIndex = 0;
                int max = Math.Min(offset + rowsPerBatch, totalRows);
                for (int i = offset; i < max; i++)
                {
                    if (i > offset) sb.Append(", ");
                    sb.Append("(");
                    int colIndex = 0;
                    foreach (DataColumn column in table.Columns)
                    {
                        var paramName = $"@p{paramIndex++}";
                        parameters[paramName] = table.Rows[i][column] ?? DBNull.Value;
                        if (colIndex > 0) sb.Append(", ");
                        sb.Append(paramName);
                        colIndex++;
                    }
                    sb.Append(")");
                }
                sb.Append(";");

                await ExecuteNonQueryAsync(connection, useTransaction ? _transaction : transaction, sb.ToString(), parameters, cancellationToken).ConfigureAwait(false);
            }

            if (!useTransaction)
            {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
                if (transaction != null)
                {
                    await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                }
#else
                transaction?.Commit();
#endif
            }
        }
        catch (Exception ex)
        {
            if (!useTransaction)
            {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
                if (transaction != null)
                {
                    await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                }
#else
                transaction?.Rollback();
#endif
            }
            throw new DbaQueryExecutionException("Failed to execute bulk insert.", destinationTable, ex);
        }
        finally
        {
            if (!useTransaction)
            {
                if (transaction != null)
                {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
                    await transaction.DisposeAsync().ConfigureAwait(false);
#else
                    transaction.Dispose();
#endif
                }
            }
            if (dispose)
            {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
                await connection.DisposeAsync().ConfigureAwait(false);
#else
                connection?.Dispose();
#endif
            }
        }
    }

    public virtual void BeginTransaction(string database)
    {
        lock (_syncRoot)
        {
            if (_transaction != null)
            {
                throw new DbaTransactionException("Transaction already started.");
            }

            var connectionString = BuildConnectionString(database);

            _transactionConnection = new SqliteConnection(connectionString);
            _transactionConnection.Open();
            _transaction = _transactionConnection.BeginTransaction();
        }
    }

    public virtual void BeginTransaction(string database, IsolationLevel isolationLevel)
        => BeginTransaction(database);

    public virtual async Task BeginTransactionAsync(string database, CancellationToken cancellationToken = default)
    {
        lock (_syncRoot)
        {
            if (_transaction != null)
            {
                throw new DbaTransactionException("Transaction already started.");
            }
        }

        var connectionString = BuildConnectionString(database);

        var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
        var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
#else
        var transaction = connection.BeginTransaction();
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

    public virtual Task BeginTransactionAsync(string database, IsolationLevel isolationLevel, CancellationToken cancellationToken = default)
        => BeginTransactionAsync(database, cancellationToken);

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
        SqliteTransaction? tx;
        SqliteConnection? conn;
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
        SqliteTransaction? tx;
        SqliteConnection? conn;
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

    protected override bool IsTransient(Exception ex) =>
        ex is SqliteException sqliteEx &&
        sqliteEx.SqliteErrorCode is 5 or 6;

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            DisposeTransaction();
        }
        base.Dispose(disposing);
    }

    public async Task<IReadOnlyList<object?>> RunQueriesInParallel(IEnumerable<string> queries, string database, CancellationToken cancellationToken = default, int? maxDegreeOfParallelism = null)
    {
        if (queries == null)
        {
            throw new ArgumentNullException(nameof(queries));
        }

        SemaphoreSlim? throttler = null;
        if (maxDegreeOfParallelism.HasValue && maxDegreeOfParallelism.Value > 0)
        {
            throttler = new SemaphoreSlim(maxDegreeOfParallelism.Value);
        }

        var tasks = queries.Select(async q =>
        {
            if (throttler != null)
            {
                await throttler.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            try
            {
                return await QueryAsync(database, q, null, false, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                throttler?.Release();
            }
        });

        var results = await Task.WhenAll(tasks).ConfigureAwait(false);
        throttler?.Dispose();
        return results;
    }
}
