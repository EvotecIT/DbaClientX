using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
using System.Runtime.CompilerServices;
#endif

namespace DBAClientX;

/// <summary>
/// Provides a common foundation for database client implementations, including
/// retry logic, parameter handling, and result materialization helpers.
/// </summary>
public abstract class DatabaseClientBase : IDisposable
{
    private readonly object _syncRoot = new();
    private ReturnType _returnType;
    private int _commandTimeout;
    private int _maxRetryAttempts = 3;
    private TimeSpan _retryDelay = TimeSpan.FromMilliseconds(200);
    private bool _disposed;

    private const int MaxBackoffMilliseconds = 30000; // cap backoff to 30s
    private static readonly ThreadLocal<Random> _rand = new(() => new Random(unchecked(Environment.TickCount * 31 + Thread.CurrentThread.ManagedThreadId)));

    /// <summary>
    /// Gets or sets the desired return type for query executions.
    /// </summary>
    public ReturnType ReturnType
    {
        get { lock (_syncRoot) { return _returnType; } }
        set { lock (_syncRoot) { _returnType = value; } }
    }

    /// <summary>
    /// Gets or sets the command timeout applied to database commands, in seconds.
    /// Specify <c>0</c> to use the provider default.
    /// </summary>
    public int CommandTimeout
    {
        get { lock (_syncRoot) { return _commandTimeout; } }
        set { lock (_syncRoot) { _commandTimeout = value; } }
    }

    /// <summary>
    /// Gets or sets the maximum number of retry attempts for transient failures.
    /// A value lower than <c>1</c> is treated as a single attempt.
    /// </summary>
    public int MaxRetryAttempts
    {
        get { lock (_syncRoot) { return _maxRetryAttempts; } }
        set
        {
            if (value < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(value), "MaxRetryAttempts cannot be negative.");
            }
            lock (_syncRoot) { _maxRetryAttempts = value; }
        }
    }

    /// <summary>
    /// Gets or sets the base delay between retry attempts. Exponential backoff
    /// with jitter is derived from this value.
    /// </summary>
    public TimeSpan RetryDelay
    {
        get { lock (_syncRoot) { return _retryDelay; } }
        set { lock (_syncRoot) { _retryDelay = value; } }
    }

    /// <summary>
    /// Determines whether an exception represents a transient failure that warrants a retry.
    /// </summary>
    /// <param name="ex">The exception encountered during execution.</param>
    /// <returns><c>true</c> when the exception is transient; otherwise, <c>false</c>.</returns>
    protected virtual bool IsTransient(Exception ex) => false;

    /// <summary>
    /// Executes an operation with retry logic for transient failures.
    /// </summary>
    /// <typeparam name="T">The type of the result produced by the operation.</typeparam>
    /// <param name="operation">The operation to execute.</param>
    /// <returns>The result of the successful operation.</returns>
    /// <exception cref="Exception">Thrown when all retry attempts fail.</exception>
    protected T ExecuteWithRetry<T>(Func<T> operation)
    {
        var attempts = 0;
        Exception? lastException = null;
        var maxAttempts = MaxRetryAttempts < 1 ? 1 : MaxRetryAttempts;
        while (attempts < maxAttempts)
        {
            try
            {
                return operation();
            }
            catch (Exception ex) when (IsTransient(ex))
            {
                lastException = ex;
                attempts++;
                if (attempts >= maxAttempts)
                {
                    break;
                }
                var delay = ComputeBackoffDelay(attempts);
                if (delay > TimeSpan.Zero)
                {
                    Thread.Sleep(delay);
                }
            }
        }
        throw lastException ?? new Exception("Operation failed.");
    }

    /// <summary>
    /// Asynchronously executes an operation with retry logic for transient failures.
    /// </summary>
    /// <typeparam name="T">The type of the result produced by the operation.</typeparam>
    /// <param name="operation">The asynchronous operation to execute.</param>
    /// <param name="cancellationToken">Token used to cancel the retries.</param>
    /// <returns>The result of the successful operation.</returns>
    /// <exception cref="Exception">Thrown when all retry attempts fail.</exception>
    protected async Task<T> ExecuteWithRetryAsync<T>(Func<Task<T>> operation, CancellationToken cancellationToken = default)
    {
        var attempts = 0;
        Exception? lastException = null;
        var maxAttempts = MaxRetryAttempts < 1 ? 1 : MaxRetryAttempts;
        while (attempts < maxAttempts)
        {
            try
            {
                return await operation().ConfigureAwait(false);
            }
            catch (Exception ex) when (IsTransient(ex))
            {
                lastException = ex;
                attempts++;
                if (attempts >= maxAttempts || cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                var delay = ComputeBackoffDelay(attempts);
                if (delay > TimeSpan.Zero)
                {
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        throw lastException ?? new Exception("Operation failed.");
    }

    private TimeSpan ComputeBackoffDelay(int attempt)
    {
        var baseDelay = RetryDelay;
        if (baseDelay <= TimeSpan.Zero)
        {
            return TimeSpan.Zero;
        }
        // Exponential backoff with jitter (full jitter)
        var factor = Math.Pow(2, Math.Max(0, attempt - 1));
        var ms = baseDelay.TotalMilliseconds * factor;
        var jitter = _rand.Value!.NextDouble() * baseDelay.TotalMilliseconds; // 0..base
        var total = Math.Min(ms + jitter, MaxBackoffMilliseconds);
        return TimeSpan.FromMilliseconds(total);
    }

    /// <summary>
    /// Adds parameters created from dictionaries of values, types, and directions to the supplied command.
    /// </summary>
    /// <param name="command">The command receiving the parameters.</param>
    /// <param name="parameters">Parameter values keyed by parameter name.</param>
    /// <param name="parameterTypes">Explicit database types keyed by parameter name.</param>
    /// <param name="parameterDirections">Explicit directions keyed by parameter name.</param>
    protected virtual void AddParameters(DbCommand command, IDictionary<string, object?>? parameters, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        if (parameters == null)
        {
            return;
        }

        foreach (var pair in parameters)
        {
            var value = pair.Value ?? DBNull.Value;
            var parameter = command.CreateParameter();
            parameter.ParameterName = pair.Key;
            parameter.Value = value;
            if (parameterTypes != null && parameterTypes.TryGetValue(pair.Key, out var explicitType))
            {
                parameter.DbType = explicitType;
            }
            else
            {
                parameter.DbType = InferDbType(value);
            }
            if (parameterDirections != null && parameterDirections.TryGetValue(pair.Key, out var direction))
            {
                parameter.Direction = direction;
            }
            command.Parameters.Add(parameter);
        }
    }

    /// <summary>
    /// Copies the values of output parameters back into the supplied dictionary.
    /// </summary>
    /// <param name="command">The command that was executed.</param>
    /// <param name="parameters">The dictionary receiving updated parameter values.</param>
    protected virtual void UpdateOutputParameters(DbCommand command, IDictionary<string, object?>? parameters)
    {
        if (parameters == null)
        {
            return;
        }
        foreach (DbParameter p in command.Parameters)
        {
            if (p.Direction != ParameterDirection.Input)
            {
                parameters[p.ParameterName] = p.Value == DBNull.Value ? null : p.Value;
            }
        }
    }

    /// <summary>
    /// Adds pre-created parameters to the supplied command.
    /// </summary>
    /// <param name="command">The command receiving the parameters.</param>
    /// <param name="parameters">The parameters to add.</param>
    protected virtual void AddParameters(DbCommand command, IEnumerable<DbParameter>? parameters)
    {
        if (parameters == null)
        {
            return;
        }

        foreach (var parameter in parameters)
        {
            command.Parameters.Add(parameter);
        }
    }

    private static DbType InferDbType(object? value)
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

    /// <summary>
    /// Executes a query and materializes the result according to <see cref="ReturnType"/>.
    /// </summary>
    /// <param name="connection">The open database connection.</param>
    /// <param name="transaction">The transaction to enlist in, if any.</param>
    /// <param name="query">The query to execute.</param>
    /// <param name="parameters">Optional parameter values.</param>
    /// <param name="parameterTypes">Optional parameter types.</param>
    /// <param name="parameterDirections">Optional parameter directions.</param>
    /// <returns>The query result shaped according to <see cref="ReturnType"/>.</returns>
    protected virtual object? ExecuteQuery(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        return ExecuteWithRetry<object?>(() =>
        {
            using var command = connection.CreateCommand();
            command.CommandText = query;
            command.Transaction = transaction;
            AddParameters(command, parameters, parameterTypes, parameterDirections);
            var commandTimeout = CommandTimeout;
            if (commandTimeout > 0)
            {
                command.CommandTimeout = commandTimeout;
            }

            using var reader = command.ExecuteReader();
            var returnType = ReturnType;
            if (returnType == ReturnType.DataRow)
            {
                if (reader.Read())
                {
                    var table = new DataTable("Table0");
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        table.Columns.Add(reader.GetName(i), reader.GetFieldType(i));
                    }
                    var row = table.NewRow();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        row[i] = reader.IsDBNull(i) ? DBNull.Value : reader.GetValue(i);
                    }
                    return row;
                }
                return null;
            }
            if (returnType == ReturnType.DataTable || returnType == ReturnType.PSObject)
            {
                var table = new DataTable("Table0");
                table.Load(reader);
                return table;
            }

            var dataSet = new DataSet();
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
        });
    }

    /// <summary>
    /// Executes a non-query command (INSERT/UPDATE/DELETE) with retry support.
    /// </summary>
    /// <param name="connection">The open database connection.</param>
    /// <param name="transaction">The transaction to enlist in, if any.</param>
    /// <param name="query">The command text to execute.</param>
    /// <param name="parameters">Optional parameter values.</param>
    /// <param name="parameterTypes">Optional parameter types.</param>
    /// <param name="parameterDirections">Optional parameter directions.</param>
    /// <returns>The number of rows affected.</returns>
    protected virtual int ExecuteNonQuery(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        return ExecuteWithRetry(() =>
        {
            using var command = connection.CreateCommand();
            command.CommandText = query;
            command.Transaction = transaction;
            AddParameters(command, parameters, parameterTypes, parameterDirections);
            var commandTimeout = CommandTimeout;
            if (commandTimeout > 0)
            {
                command.CommandTimeout = commandTimeout;
            }

            var affected = command.ExecuteNonQuery();
            UpdateOutputParameters(command, parameters);
            return affected;
        });
    }

    /// <summary>
    /// Executes a scalar command and returns the first column of the first row.
    /// </summary>
    /// <param name="connection">The open database connection.</param>
    /// <param name="transaction">The transaction to enlist in, if any.</param>
    /// <param name="query">The command text to execute.</param>
    /// <param name="parameters">Optional parameter values.</param>
    /// <param name="parameterTypes">Optional parameter types.</param>
    /// <param name="parameterDirections">Optional parameter directions.</param>
    /// <returns>The scalar result.</returns>
    protected virtual object? ExecuteScalar(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        return ExecuteWithRetry<object?>(() =>
        {
            using var command = connection.CreateCommand();
            command.CommandText = query;
            command.Transaction = transaction;
            AddParameters(command, parameters, parameterTypes, parameterDirections);
            var commandTimeout = CommandTimeout;
            if (commandTimeout > 0)
            {
                command.CommandTimeout = commandTimeout;
            }
            var result = command.ExecuteScalar();
            UpdateOutputParameters(command, parameters);
            return result;
        });
    }

    /// <summary>
    /// Asynchronously executes a query and materializes the result according to <see cref="ReturnType"/>.
    /// </summary>
    /// <param name="connection">The open database connection.</param>
    /// <param name="transaction">The transaction to enlist in, if any.</param>
    /// <param name="query">The query to execute.</param>
    /// <param name="parameters">Optional parameter values.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <param name="parameterTypes">Optional parameter types.</param>
    /// <param name="parameterDirections">Optional parameter directions.</param>
    /// <returns>The query result shaped according to <see cref="ReturnType"/>.</returns>
    protected virtual async Task<object?> ExecuteQueryAsync(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, CancellationToken cancellationToken = default, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        return await ExecuteWithRetryAsync(async () =>
        {
            using var command = connection.CreateCommand();
            command.CommandText = query;
            command.Transaction = transaction;
            AddParameters(command, parameters, parameterTypes, parameterDirections);
            var commandTimeout = CommandTimeout;
            if (commandTimeout > 0)
            {
                command.CommandTimeout = commandTimeout;
            }

            using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            var returnType = ReturnType;
            if (returnType == ReturnType.DataRow)
            {
                if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    var table = new DataTable("Table0");
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        table.Columns.Add(reader.GetName(i), reader.GetFieldType(i));
                    }
                    var row = table.NewRow();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        row[i] = reader.IsDBNull(i) ? DBNull.Value : reader.GetValue(i);
                    }
                    return row;
                }
                return null;
            }
            if (returnType == ReturnType.DataTable || returnType == ReturnType.PSObject)
            {
                var table = new DataTable("Table0");
                table.Load(reader);
                return table;
            }

            var dataSet = new DataSet();
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
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously executes a non-query command (INSERT/UPDATE/DELETE) with retry support.
    /// </summary>
    /// <param name="connection">The open database connection.</param>
    /// <param name="transaction">The transaction to enlist in, if any.</param>
    /// <param name="query">The command text to execute.</param>
    /// <param name="parameters">Optional parameter values.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <param name="parameterTypes">Optional parameter types.</param>
    /// <param name="parameterDirections">Optional parameter directions.</param>
    /// <returns>The number of rows affected.</returns>
    protected virtual async Task<int> ExecuteNonQueryAsync(
        DbConnection connection,
        DbTransaction? transaction,
        string query,
        IDictionary<string, object?>? parameters = null,
        CancellationToken cancellationToken = default,
        IDictionary<string, DbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        return await ExecuteWithRetryAsync(async () =>
        {
            using var command = connection.CreateCommand();
            command.CommandText = query;
            command.Transaction = transaction;
            AddParameters(command, parameters, parameterTypes, parameterDirections);
            var commandTimeout = CommandTimeout;
            if (commandTimeout > 0)
            {
                command.CommandTimeout = commandTimeout;
            }

            var affected = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            UpdateOutputParameters(command, parameters);
            return affected;
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously executes a scalar command and returns the first column of the first row.
    /// </summary>
    /// <param name="connection">The open database connection.</param>
    /// <param name="transaction">The transaction to enlist in, if any.</param>
    /// <param name="query">The command text to execute.</param>
    /// <param name="parameters">Optional parameter values.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <param name="parameterTypes">Optional parameter types.</param>
    /// <param name="parameterDirections">Optional parameter directions.</param>
    /// <returns>The scalar result.</returns>
    protected virtual async Task<object?> ExecuteScalarAsync(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, CancellationToken cancellationToken = default, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        return await ExecuteWithRetryAsync(async () =>
        {
            using var command = connection.CreateCommand();
            command.CommandText = query;
            command.Transaction = transaction;
            AddParameters(command, parameters, parameterTypes, parameterDirections);
            var commandTimeout = CommandTimeout;
            if (commandTimeout > 0)
            {
                command.CommandTimeout = commandTimeout;
            }

            var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            UpdateOutputParameters(command, parameters);
            return result;
        }, cancellationToken).ConfigureAwait(false);
    }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
    /// <summary>
    /// Asynchronously executes a query and streams rows using <see cref="IAsyncEnumerable{T}"/>.
    /// </summary>
    /// <param name="connection">The open database connection.</param>
    /// <param name="transaction">The transaction to enlist in, if any.</param>
    /// <param name="query">The query to execute.</param>
    /// <param name="parameters">Optional parameter values.</param>
    /// <param name="cancellationToken">Token used to cancel the iteration.</param>
    /// <param name="parameterTypes">Optional parameter types.</param>
    /// <param name="parameterDirections">Optional parameter directions.</param>
    /// <param name="dbParameters">Provider-specific parameters to attach directly to the command.</param>
    /// <param name="commandType">Command type to use (Text or StoredProcedure).</param>
    /// <returns>An asynchronous stream of <see cref="DataRow"/> instances.</returns>
    protected virtual async IAsyncEnumerable<DataRow> ExecuteQueryStreamAsync(
        DbConnection connection,
        DbTransaction? transaction,
        string query,
        IDictionary<string, object?>? parameters = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default,
        IDictionary<string, DbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null,
        IEnumerable<DbParameter>? dbParameters = null,
        CommandType commandType = CommandType.Text)
    {
        var maxAttempts = MaxRetryAttempts < 1 ? 1 : MaxRetryAttempts;
        var attempt = 0;

        using var command = connection.CreateCommand();
        command.CommandText = query;
        command.Transaction = transaction;
        command.CommandType = commandType;
        AddParameters(command, parameters, parameterTypes, parameterDirections);
        AddParameters(command, dbParameters);
        var commandTimeout = CommandTimeout;
        if (commandTimeout > 0)
        {
            command.CommandTimeout = commandTimeout;
        }

        async Task<DbDataReader> OpenReaderAsync()
        {
            while (true)
            {
                try
                {
                    return await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex) when (IsTransient(ex) && ++attempt < maxAttempts)
                {
                    var delay = ComputeBackoffDelay(attempt);
                    if (delay > TimeSpan.Zero)
                    {
                        await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                    }
                }
            }
        }

        await using var reader = await OpenReaderAsync().ConfigureAwait(false);

        var table = new DataTable();
        for (int i = 0; i < reader.FieldCount; i++)
        {
            table.Columns.Add(reader.GetName(i), reader.GetFieldType(i));
        }

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var row = table.NewRow();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                row[i] = reader.IsDBNull(i) ? DBNull.Value : reader.GetValue(i);
            }
            yield return row;
        }

        UpdateOutputParameters(command, parameters);
        yield break;
    }
#endif

    /// <summary>
    /// Shapes a <see cref="DataSet"/> into the configured <see cref="ReturnType"/>.
    /// </summary>
    /// <param name="dataSet">The data set produced by a query.</param>
    /// <returns>The result object appropriate for the configured return type.</returns>
    protected object? BuildResult(DataSet dataSet)
    {
        var returnType = ReturnType;
        if (returnType == ReturnType.DataRow)
        {
            if (dataSet.Tables.Count > 0 && dataSet.Tables[0].Rows.Count > 0)
            {
                return dataSet.Tables[0].Rows[0];
            }
        }
        else if (returnType == ReturnType.DataTable || returnType == ReturnType.PSObject)
        {
            if (dataSet.Tables.Count > 0)
            {
                return dataSet.Tables[0];
            }
        }
        else if (returnType == ReturnType.DataSet)
        {
            return dataSet;
        }
        return null;
    }

    /// <summary>
    /// Disposes the instance and suppresses finalization.
    /// </summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Releases managed resources. Override to dispose additional state.
    /// </summary>
    /// <param name="disposing">Indicates whether the method is invoked from <see cref="Dispose()"/>.</param>
    protected virtual void Dispose(bool disposing)
    {
        if (_disposed)
        {
            return;
        }
        _disposed = true;
    }
}

