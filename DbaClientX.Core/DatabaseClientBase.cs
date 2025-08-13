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

public abstract class DatabaseClientBase : IDisposable
{
    private readonly object _syncRoot = new();
    private ReturnType _returnType;
    private int _commandTimeout;
    private int _maxRetryAttempts = 3;
    private TimeSpan _retryDelay = TimeSpan.FromMilliseconds(200);
    private bool _disposed;

    public ReturnType ReturnType
    {
        get { lock (_syncRoot) { return _returnType; } }
        set { lock (_syncRoot) { _returnType = value; } }
    }

    public int CommandTimeout
    {
        get { lock (_syncRoot) { return _commandTimeout; } }
        set { lock (_syncRoot) { _commandTimeout = value; } }
    }

    public int MaxRetryAttempts
    {
        get { lock (_syncRoot) { return _maxRetryAttempts; } }
        set { lock (_syncRoot) { _maxRetryAttempts = value; } }
    }

    public TimeSpan RetryDelay
    {
        get { lock (_syncRoot) { return _retryDelay; } }
        set { lock (_syncRoot) { _retryDelay = value; } }
    }

    protected virtual bool IsTransient(Exception ex) => false;

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
                var delay = RetryDelay;
                if (delay > TimeSpan.Zero)
                {
                    Thread.Sleep(delay);
                }
            }
        }
        throw lastException ?? new Exception("Operation failed.");
    }

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
                var delay = RetryDelay;
                if (delay > TimeSpan.Zero)
                {
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        throw lastException ?? new Exception("Operation failed.");
    }

    protected virtual void AddParameters(DbCommand command, IDictionary<string, object?>? parameters, IDictionary<string, DbType>? parameterTypes = null)
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
            command.Parameters.Add(parameter);
        }
    }

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
        return Type.GetTypeCode(value.GetType()) switch
        {
            TypeCode.Byte => DbType.Byte,
            TypeCode.Int16 => DbType.Int16,
            TypeCode.Int32 => DbType.Int32,
            TypeCode.Int64 => DbType.Int64,
            TypeCode.Decimal => DbType.Decimal,
            TypeCode.Double => DbType.Double,
            TypeCode.Single => DbType.Single,
            TypeCode.Boolean => DbType.Boolean,
            TypeCode.String => DbType.String,
            TypeCode.DateTime => DbType.DateTime,
            _ => DbType.Object
        };
    }

    protected virtual object? ExecuteQuery(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, IDictionary<string, DbType>? parameterTypes = null)
    {
        return ExecuteWithRetry<object?>(() =>
        {
            using var command = connection.CreateCommand();
            command.CommandText = query;
            command.Transaction = transaction;
            AddParameters(command, parameters, parameterTypes);
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
        });
    }

    protected virtual int ExecuteNonQuery(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, IDictionary<string, DbType>? parameterTypes = null)
    {
        return ExecuteWithRetry(() =>
        {
            using var command = connection.CreateCommand();
            command.CommandText = query;
            command.Transaction = transaction;
            AddParameters(command, parameters, parameterTypes);
            var commandTimeout = CommandTimeout;
            if (commandTimeout > 0)
            {
                command.CommandTimeout = commandTimeout;
            }

            return command.ExecuteNonQuery();
        });
    }

    protected virtual object? ExecuteScalar(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, IDictionary<string, DbType>? parameterTypes = null)
    {
        return ExecuteWithRetry<object?>(() =>
        {
            using var command = connection.CreateCommand();
            command.CommandText = query;
            command.Transaction = transaction;
            AddParameters(command, parameters, parameterTypes);
            var commandTimeout = CommandTimeout;
            if (commandTimeout > 0)
            {
                command.CommandTimeout = commandTimeout;
            }

            return command.ExecuteScalar();
        });
    }

    protected virtual async Task<object?> ExecuteQueryAsync(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, CancellationToken cancellationToken = default, IDictionary<string, DbType>? parameterTypes = null)
    {
        return await ExecuteWithRetryAsync(async () =>
        {
            using var command = connection.CreateCommand();
            command.CommandText = query;
            command.Transaction = transaction;
            AddParameters(command, parameters, parameterTypes);
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
        }, cancellationToken).ConfigureAwait(false);
    }

    protected virtual async Task<object?> ExecuteScalarAsync(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, CancellationToken cancellationToken = default, IDictionary<string, DbType>? parameterTypes = null)
    {
        return await ExecuteWithRetryAsync(async () =>
        {
            using var command = connection.CreateCommand();
            command.CommandText = query;
            command.Transaction = transaction;
            AddParameters(command, parameters, parameterTypes);
            var commandTimeout = CommandTimeout;
            if (commandTimeout > 0)
            {
                command.CommandTimeout = commandTimeout;
            }

            return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        }, cancellationToken).ConfigureAwait(false);
    }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
    protected virtual async IAsyncEnumerable<DataRow> ExecuteQueryStreamAsync(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, [EnumeratorCancellation] CancellationToken cancellationToken = default, IDictionary<string, DbType>? parameterTypes = null)
    {
        var maxAttempts = MaxRetryAttempts < 1 ? 1 : MaxRetryAttempts;
        var attempt = 0;

        using var command = connection.CreateCommand();
        command.CommandText = query;
        command.Transaction = transaction;
        AddParameters(command, parameters, parameterTypes);
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
                    var delay = RetryDelay;
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
        yield break;
    }
#endif

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

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (_disposed)
        {
            return;
        }
        _disposed = true;
    }
}

