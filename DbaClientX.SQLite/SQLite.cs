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
/// Provides the SQLite-specific implementation of <see cref="DatabaseClientBase"/> exposing
/// convenience helpers for executing commands, queries, and bulk operations against a local
/// or remote SQLite database file.
/// </summary>
public class SQLite : DatabaseClientBase
{
    private readonly object _syncRoot = new();
    private SqliteConnection? _transactionConnection;
    private SqliteTransaction? _transaction;

    /// <summary>
    /// Gets a value indicating whether an explicit transaction scope is currently active.
    /// </summary>
    /// <remarks>
    /// The flag is toggled by the <see cref="BeginTransaction(string)"/>,
    /// <see cref="BeginTransactionAsync(string, System.Threading.CancellationToken)"/> and related overloads
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
    /// error information is required, invoke <see cref="ExecuteScalar(string, string, IDictionary{string, object?}? , bool, IDictionary{string, SqliteType}?, IDictionary{string, ParameterDirection}?)"/>
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

    /// <summary>
    /// Executes a SQL query and materializes the result using the shared <see cref="DatabaseClientBase"/> pipeline.
    /// </summary>
    /// <param name="database">Absolute or relative path of the SQLite database file.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional parameter map providing values for <paramref name="query"/>.</param>
    /// <param name="useTransaction">Indicates whether the currently active transaction should be used.</param>
    /// <param name="parameterTypes">Optional map of parameter names to <see cref="SqliteType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter names to <see cref="ParameterDirection"/> values.</param>
    /// <returns>The materialized query result as determined by <see cref="DatabaseClientBase.ExecuteQuery(DbConnection, DbTransaction?, string, IDictionary{string, object?}?, IDictionary{string, DbType}?, IDictionary{string, ParameterDirection}?)"/>.</returns>
    /// <remarks>
    /// Always supply <paramref name="parameters"/> (and optionally <paramref name="parameterTypes"/>) when incorporating user
    /// input to prevent SQL injection vulnerabilities. Reuse an explicit transaction when executing multiple statements that
    /// must succeed or fail atomically.
    /// </remarks>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the command fails to execute.</exception>
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

    /// <summary>
    /// Executes a SQL query that returns a single scalar value.
    /// </summary>
    /// <param name="database">Absolute or relative path of the SQLite database file.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional parameter map providing values for <paramref name="query"/>.</param>
    /// <param name="useTransaction">Indicates whether the currently active transaction should be used.</param>
    /// <param name="parameterTypes">Optional map of parameter names to <see cref="SqliteType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter names to <see cref="ParameterDirection"/> values.</param>
    /// <returns>The first column of the first row from the result set or <see langword="null"/> when no data is returned.</returns>
    /// <remarks>
    /// Use this method for aggregate queries (for example <c>SELECT COUNT(*)</c>) or statements that return a single value. It
    /// mirrors <see cref="Query(string, string, IDictionary{string, object?}?, bool, IDictionary{string, SqliteType}?, IDictionary{string, ParameterDirection}?)"/>
    /// but short-circuits the materialization logic to minimize allocations.
    /// </remarks>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the command fails to execute.</exception>
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

    /// <summary>
    /// Executes a SQL statement that does not return rows (for example <c>INSERT</c>, <c>UPDATE</c>, or <c>DELETE</c>).
    /// </summary>
    /// <param name="database">Absolute or relative path of the SQLite database file.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional parameter map providing values for <paramref name="query"/>.</param>
    /// <param name="useTransaction">Indicates whether the currently active transaction should be used.</param>
    /// <param name="parameterTypes">Optional map of parameter names to <see cref="SqliteType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter names to <see cref="ParameterDirection"/> values.</param>
    /// <returns>The number of rows affected by the operation.</returns>
    /// <remarks>
    /// Use transactions to wrap dependent mutations; otherwise a failure halfway through a batch of statements may leave the
    /// database in an inconsistent state. When you do not pass <paramref name="useTransaction"/>, the method opens and disposes
    /// a dedicated connection for the call.
    /// </remarks>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the command fails to execute.</exception>
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

    /// <summary>
    /// Asynchronously executes a SQL statement that does not return rows.
    /// </summary>
    /// <param name="database">Absolute or relative path of the SQLite database file.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional parameter map providing values for <paramref name="query"/>.</param>
    /// <param name="useTransaction">Indicates whether the currently active transaction should be used.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <param name="parameterTypes">Optional map of parameter names to <see cref="SqliteType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter names to <see cref="ParameterDirection"/> values.</param>
    /// <returns>A task producing the number of rows affected by the operation.</returns>
    /// <remarks>
    /// Mirrors the synchronous <see cref="ExecuteNonQuery(string, string, IDictionary{string, object?}?, bool, IDictionary{string, SqliteType}?, IDictionary{string, ParameterDirection}?)"/> method while leveraging asynchronous I/O to keep threads available.
    /// </remarks>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the command fails to execute.</exception>
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

    /// <summary>
    /// Asynchronously executes a SQL query and materializes the result using the shared pipeline from <see cref="DatabaseClientBase"/>.
    /// </summary>
    /// <param name="database">Absolute or relative path of the SQLite database file.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional parameter map providing values for <paramref name="query"/>.</param>
    /// <param name="useTransaction">Indicates whether the currently active transaction should be used.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <param name="parameterTypes">Optional map of parameter names to <see cref="SqliteType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter names to <see cref="ParameterDirection"/> values.</param>
    /// <returns>A task producing the materialized query result.</returns>
    /// <remarks>
    /// Prefer this overload when running in asynchronous-capable environments (such as ASP.NET). It keeps the calling thread
    /// responsive while awaiting data retrieval.
    /// </remarks>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the command fails to execute.</exception>
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

    /// <summary>
    /// Asynchronously executes a SQL query that returns a single scalar value.
    /// </summary>
    /// <param name="database">Absolute or relative path of the SQLite database file.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional parameter map providing values for <paramref name="query"/>.</param>
    /// <param name="useTransaction">Indicates whether the currently active transaction should be used.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <param name="parameterTypes">Optional map of parameter names to <see cref="SqliteType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter names to <see cref="ParameterDirection"/> values.</param>
    /// <returns>A task producing the scalar result or <see langword="null"/> when no data is returned.</returns>
    /// <remarks>
    /// Ideal for aggregate lookups executed in asynchronous call stacks. The method uses the same safety checks and exception
    /// behavior as <see cref="ExecuteScalar(string, string, IDictionary{string, object?}?, bool, IDictionary{string, SqliteType}?, IDictionary{string, ParameterDirection}?)"/>.
    /// </remarks>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the command fails to execute.</exception>
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
    /// <summary>
    /// Streams query results asynchronously, yielding one <see cref="DataRow"/> at a time.
    /// </summary>
    /// <param name="database">Absolute or relative path of the SQLite database file.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional parameter map providing values for <paramref name="query"/>.</param>
    /// <param name="useTransaction">Indicates whether the currently active transaction should be used.</param>
    /// <param name="cancellationToken">Token used to cancel iteration of the result stream.</param>
    /// <param name="parameterTypes">Optional map of parameter names to <see cref="SqliteType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter names to <see cref="ParameterDirection"/> values.</param>
    /// <returns>An <see cref="IAsyncEnumerable{T}"/> producing rows as they become available.</returns>
    /// <remarks>
    /// This method is available on TFMs that support <see cref="IAsyncEnumerable{T}"/>. It is particularly beneficial when
    /// processing large data sets because rows are surfaced as soon as they are read instead of buffering the entire result set
    /// in memory.
    /// </remarks>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the command fails to execute.</exception>
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

    /// <summary>
    /// Inserts all rows from the supplied <see cref="DataTable"/> into the specified destination table.
    /// </summary>
    /// <param name="database">Absolute or relative path of the SQLite database file.</param>
    /// <param name="table">Table whose rows will be written to the database.</param>
    /// <param name="destinationTable">Name of the destination table.</param>
    /// <param name="useTransaction">Indicates whether the currently active transaction should be used.</param>
    /// <param name="batchSize">Optional number of rows to group into a single multi-value <c>INSERT</c> statement.</param>
    /// <remarks>
    /// SQLite does not offer a native bulk API. This helper builds batched multi-row <c>INSERT</c> statements to reduce round trips.
    /// When <paramref name="useTransaction"/> is <see langword="false"/>, the method opens a dedicated transaction internally
    /// so that either all rows succeed or none are written.
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="table"/> is <see langword="null"/>.</exception>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the bulk insert fails.</exception>
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

    /// <summary>
    /// Asynchronously inserts all rows from the supplied <see cref="DataTable"/> into the specified destination table.
    /// </summary>
    /// <param name="database">Absolute or relative path of the SQLite database file.</param>
    /// <param name="table">Table whose rows will be written to the database.</param>
    /// <param name="destinationTable">Name of the destination table.</param>
    /// <param name="useTransaction">Indicates whether the currently active transaction should be used.</param>
    /// <param name="batchSize">Optional number of rows to group into a single multi-value <c>INSERT</c> statement.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <remarks>
    /// Mirrors <see cref="BulkInsert(string, DataTable, string, bool, int?)"/> while using asynchronous data reader APIs where available.
    /// When <paramref name="useTransaction"/> is <see langword="false"/>, a local transaction scope is created to guarantee atomicity.
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="table"/> is <see langword="null"/>.</exception>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the bulk insert fails.</exception>
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

    /// <summary>
    /// Starts a new transaction using a dedicated <see cref="SqliteConnection"/> targeting the provided database.
    /// </summary>
    /// <param name="database">Absolute or relative path of the SQLite database file.</param>
    /// <remarks>
    /// Transactions ensure that a group of statements either all succeed or none are applied. Call <see cref="Commit"/>
    /// or <see cref="Rollback"/> once the unit of work has completed. The SQLite provider uses a single shared transaction
    /// per client instance; invoking this method multiple times without committing results in a <see cref="DbaTransactionException"/>.
    /// </remarks>
    /// <exception cref="DbaTransactionException">Thrown when a transaction is already active.</exception>
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

    /// <summary>
    /// Starts a new transaction using the provider default isolation semantics.
    /// </summary>
    /// <param name="database">Absolute or relative path of the SQLite database file.</param>
    /// <param name="isolationLevel">Ignored. Provided to keep parity with other database providers.</param>
    /// <remarks>
    /// SQLite only exposes a limited set of isolation levels. The provider honors the value supplied when possible but falls back
    /// to the default behavior. Prefer <see cref="BeginTransaction(string)"/> for clarity.
    /// </remarks>
    /// <exception cref="DbaTransactionException">Thrown when a transaction is already active.</exception>
    public virtual void BeginTransaction(string database, IsolationLevel isolationLevel)
        => BeginTransaction(database);

    /// <summary>
    /// Asynchronously starts a new transaction using a dedicated <see cref="SqliteConnection"/> targeting the provided database.
    /// </summary>
    /// <param name="database">Absolute or relative path of the SQLite database file.</param>
    /// <param name="cancellationToken">Token used to cancel the connection or transaction creation.</param>
    /// <remarks>
    /// Mirrors <see cref="BeginTransaction(string)"/> but leverages asynchronous calls to avoid blocking threads while opening
    /// the connection or initializing the transaction scope.
    /// </remarks>
    /// <exception cref="DbaTransactionException">Thrown when a transaction is already active.</exception>
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

    /// <summary>
    /// Asynchronously starts a new transaction using the provider default isolation semantics.
    /// </summary>
    /// <param name="database">Absolute or relative path of the SQLite database file.</param>
    /// <param name="isolationLevel">Ignored. Provided to keep parity with other database providers.</param>
    /// <param name="cancellationToken">Token used to cancel the connection or transaction creation.</param>
    /// <remarks>
    /// Provided for API symmetry with other database providers even though SQLite exposes limited isolation options.
    /// </remarks>
    /// <exception cref="DbaTransactionException">Thrown when a transaction is already active.</exception>
    public virtual Task BeginTransactionAsync(string database, IsolationLevel isolationLevel, CancellationToken cancellationToken = default)
        => BeginTransactionAsync(database, cancellationToken);

    /// <summary>
    /// Commits the currently active transaction.
    /// </summary>
    /// <remarks>
    /// Releases resources associated with the transaction and closes the dedicated connection that was opened by
    /// <see cref="BeginTransaction(string)"/> or <see cref="BeginTransactionAsync(string, System.Threading.CancellationToken)"/>.
    /// </remarks>
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
    /// Asynchronously commits the currently active transaction.
    /// </summary>
    /// <param name="cancellationToken">Token used to cancel the commit operation.</param>
    /// <remarks>
    /// Mirrors <see cref="Commit"/> while allowing the caller to stay responsive. The method disposes of the underlying
    /// connection regardless of whether the commit succeeds.
    /// </remarks>
    /// <exception cref="DbaTransactionException">Thrown when no transaction is active.</exception>
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

    /// <summary>
    /// Rolls back the currently active transaction.
    /// </summary>
    /// <remarks>
    /// Use rollback when a command within the transaction fails or when business logic determines the state change should
    /// be discarded. The transaction connection is disposed once the rollback completes.
    /// </remarks>
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
    /// Asynchronously rolls back the currently active transaction.
    /// </summary>
    /// <param name="cancellationToken">Token used to cancel the rollback operation.</param>
    /// <remarks>
    /// Mirrors <see cref="Rollback"/> but executes asynchronously so callers can avoid blocking threads while SQLite performs
    /// the rollback. The method disposes of the underlying resources once the rollback is complete.
    /// </remarks>
    /// <exception cref="DbaTransactionException">Thrown when no transaction is active.</exception>
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

    /// <summary>
    /// Determines whether the supplied exception is transient and therefore eligible for retry logic.
    /// </summary>
    /// <param name="ex">The exception thrown by the underlying SQLite provider.</param>
    /// <returns><see langword="true"/> when <paramref name="ex"/> represents a lock or busy condition.</returns>
    protected override bool IsTransient(Exception ex) =>
        ex is SqliteException sqliteEx &&
        sqliteEx.SqliteErrorCode is 5 or 6;

    /// <summary>
    /// Releases resources associated with the SQLite client, including open transactions.
    /// </summary>
    /// <param name="disposing"><see langword="true"/> to dispose managed resources.</param>
    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            DisposeTransaction();
        }
        base.Dispose(disposing);
    }

    /// <summary>
    /// Executes a collection of SQL queries concurrently using independent connections.
    /// </summary>
    /// <param name="queries">Collection of SQL statements to execute.</param>
    /// <param name="database">Absolute or relative path of the SQLite database file.</param>
    /// <param name="cancellationToken">Token used to cancel the pending operations.</param>
    /// <param name="maxDegreeOfParallelism">Optional limit controlling how many queries may execute in parallel.</param>
    /// <returns>A list containing each query result in the same order as supplied.</returns>
    /// <remarks>
    /// This helper is useful for fan-out workloads such as running reporting queries that do not depend on one another.
    /// Consider specifying <paramref name="maxDegreeOfParallelism"/> to prevent overwhelming the host with simultaneous
    /// connections when executing large batches.
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="queries"/> is <see langword="null"/>.</exception>
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
