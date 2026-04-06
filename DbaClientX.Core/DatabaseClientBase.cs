using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Runtime.ExceptionServices;
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
public abstract class DatabaseClientBase : IDisposable, IAsyncDisposable
{
    private readonly object _syncRoot = new();
    private ReturnType _returnType;
    private int _commandTimeout;
    private int _maxRetryAttempts = 3;
    private TimeSpan _retryDelay = TimeSpan.FromMilliseconds(200);
    private bool _retryNonQueryOperations;
    private int _disposeSignaled;

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
        set
        {
            if (value < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(value), "RetryDelay cannot be negative.");
            }

            lock (_syncRoot) { _retryDelay = value; }
        }
    }

    /// <summary>
    /// Gets or sets whether mutating commands such as INSERT/UPDATE/DELETE should use automatic retries.
    /// Defaults to <see langword="false"/> to avoid replaying non-idempotent writes after partial success.
    /// </summary>
    public bool RetryNonQueryOperations
    {
        get { lock (_syncRoot) { return _retryNonQueryOperations; } }
        set { lock (_syncRoot) { _retryNonQueryOperations = value; } }
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
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                return await operation().ConfigureAwait(false);
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
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        throw lastException ?? new Exception("Operation failed.");
    }

    /// <summary>
    /// Asynchronously disposes a resource only when the current operation owns it.
    /// </summary>
    /// <typeparam name="TResource">The resource type.</typeparam>
    /// <param name="resource">The resource instance to dispose.</param>
    /// <param name="ownsResource"><see langword="true"/> when the caller created and owns the resource.</param>
    /// <param name="disposeAsync">Asynchronous disposal callback for the resource.</param>
    protected static async ValueTask DisposeOwnedResourceAsync<TResource>(TResource? resource, bool ownsResource, Func<TResource, ValueTask> disposeAsync)
        where TResource : class
    {
        if (!ownsResource || resource == null)
        {
            return;
        }

        await disposeAsync(resource).ConfigureAwait(false);
    }

    /// <summary>
    /// Disposes a pair of resources in order, skipping any <see langword="null"/> values.
    /// </summary>
    /// <typeparam name="TPrimary">The primary resource type.</typeparam>
    /// <typeparam name="TSecondary">The secondary resource type.</typeparam>
    /// <param name="primary">The primary resource instance.</param>
    /// <param name="disposePrimary">Synchronous disposal callback for the primary resource.</param>
    /// <param name="secondary">The secondary resource instance.</param>
    /// <param name="disposeSecondary">Synchronous disposal callback for the secondary resource.</param>
    protected static void DisposeResourcePair<TPrimary, TSecondary>(
        TPrimary? primary,
        Action<TPrimary> disposePrimary,
        TSecondary? secondary,
        Action<TSecondary> disposeSecondary)
        where TPrimary : class
        where TSecondary : class
    {
        if (primary != null)
        {
            disposePrimary(primary);
        }

        if (secondary != null)
        {
            disposeSecondary(secondary);
        }
    }

    /// <summary>
    /// Asynchronously disposes a pair of resources in order, skipping any <see langword="null"/> values.
    /// </summary>
    /// <typeparam name="TPrimary">The primary resource type.</typeparam>
    /// <typeparam name="TSecondary">The secondary resource type.</typeparam>
    /// <param name="primary">The primary resource instance.</param>
    /// <param name="disposePrimaryAsync">Asynchronous disposal callback for the primary resource.</param>
    /// <param name="secondary">The secondary resource instance.</param>
    /// <param name="disposeSecondaryAsync">Asynchronous disposal callback for the secondary resource.</param>
    protected static async ValueTask DisposeResourcePairAsync<TPrimary, TSecondary>(
        TPrimary? primary,
        Func<TPrimary, ValueTask> disposePrimaryAsync,
        TSecondary? secondary,
        Func<TSecondary, ValueTask> disposeSecondaryAsync)
        where TPrimary : class
        where TSecondary : class
    {
        if (primary != null)
        {
            await disposePrimaryAsync(primary).ConfigureAwait(false);
        }

        if (secondary != null)
        {
            await disposeSecondaryAsync(secondary).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Detaches the current transaction state and clears the stored references.
    /// </summary>
    /// <typeparam name="TTransaction">The provider transaction type.</typeparam>
    /// <typeparam name="TConnection">The provider connection type.</typeparam>
    /// <param name="transaction">Reference to the stored transaction field.</param>
    /// <param name="connection">Reference to the stored connection field.</param>
    /// <param name="connectionString">Reference to the stored normalized connection string field.</param>
    /// <param name="transactionInitializing">Reference to the transaction initialization flag.</param>
    /// <param name="requireActiveTransaction"><see langword="true"/> to throw when no transaction is active.</param>
    /// <returns>The detached transaction and connection pair.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="requireActiveTransaction"/> is <see langword="true"/> and no transaction is active.</exception>
    protected static (TTransaction? Transaction, TConnection? Connection) DetachTransactionState<TTransaction, TConnection>(
        ref TTransaction? transaction,
        ref TConnection? connection,
        ref string? connectionString,
        ref bool transactionInitializing,
        bool requireActiveTransaction = false)
        where TTransaction : class
        where TConnection : class
    {
        if (requireActiveTransaction && transaction == null)
        {
            throw new DbaTransactionException("No active transaction.");
        }

        var detachedTransaction = transaction;
        var detachedConnection = connection;
        transaction = null;
        connection = null;
        connectionString = null;
        transactionInitializing = false;
        return (detachedTransaction, detachedConnection);
    }

    /// <summary>
    /// Throws when a transaction is already active or currently being initialized.
    /// </summary>
    /// <typeparam name="TTransaction">The provider transaction type.</typeparam>
    /// <param name="transaction">The currently stored transaction reference.</param>
    /// <param name="transactionInitializing">The transaction initialization flag.</param>
    /// <exception cref="DbaTransactionException">Thrown when a transaction is already active or being initialized.</exception>
    protected static void EnsureTransactionStartAllowed<TTransaction>(TTransaction? transaction, bool transactionInitializing)
        where TTransaction : class
    {
        if (transaction != null || transactionInitializing)
        {
            throw new DbaTransactionException("Transaction already started.");
        }
    }

    /// <summary>
    /// Reserves transaction initialization after confirming that no transaction is active.
    /// </summary>
    /// <typeparam name="TTransaction">The provider transaction type.</typeparam>
    /// <param name="transaction">The currently stored transaction reference.</param>
    /// <param name="transactionInitializing">Reference to the transaction initialization flag.</param>
    protected static void ReserveTransactionStart<TTransaction>(TTransaction? transaction, ref bool transactionInitializing)
        where TTransaction : class
    {
        EnsureTransactionStartAllowed(transaction, transactionInitializing);
        transactionInitializing = true;
    }

    /// <summary>
    /// Stores a successfully started transaction and clears the initialization reservation.
    /// </summary>
    /// <typeparam name="TTransaction">The provider transaction type.</typeparam>
    /// <typeparam name="TConnection">The provider connection type.</typeparam>
    /// <param name="transactionField">Reference to the stored transaction field.</param>
    /// <param name="connectionField">Reference to the stored connection field.</param>
    /// <param name="connectionStringField">Reference to the stored normalized connection string field.</param>
    /// <param name="transactionInitializing">Reference to the transaction initialization flag.</param>
    /// <param name="transaction">The started transaction instance.</param>
    /// <param name="connection">The opened connection instance.</param>
    /// <param name="normalizedConnectionString">The normalized connection string associated with the transaction.</param>
    protected static void StoreStartedTransaction<TTransaction, TConnection>(
        ref TTransaction? transactionField,
        ref TConnection? connectionField,
        ref string? connectionStringField,
        ref bool transactionInitializing,
        TTransaction transaction,
        TConnection connection,
        string normalizedConnectionString)
        where TTransaction : class
        where TConnection : class
    {
        if (transactionField != null)
        {
            transactionInitializing = false;
            throw new DbaTransactionException("Transaction already started.");
        }

        connectionField = connection;
        transactionField = transaction;
        connectionStringField = normalizedConnectionString;
        transactionInitializing = false;
    }

    /// <summary>
    /// Clears the transaction initialization reservation when no active transaction was stored.
    /// </summary>
    /// <typeparam name="TTransaction">The provider transaction type.</typeparam>
    /// <param name="transaction">The currently stored transaction reference.</param>
    /// <param name="transactionInitializing">Reference to the transaction initialization flag.</param>
    protected static void ReleaseTransactionStartReservationIfNeeded<TTransaction>(TTransaction? transaction, ref bool transactionInitializing)
        where TTransaction : class
    {
        if (transaction == null)
        {
            transactionInitializing = false;
        }
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
            if (TryGetDictionaryValue(parameterTypes, pair.Key, out var explicitType))
            {
                parameter.DbType = explicitType;
            }
            else
            {
                parameter.DbType = InferDbType(value);
            }
            if (TryGetDictionaryValue(parameterDirections, pair.Key, out var direction))
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
                var targetKey = FindExistingKey(parameters, p.ParameterName) ?? p.ParameterName;
                parameters[targetKey] = p.Value == DBNull.Value ? null : p.Value;
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

    private static bool TryGetDictionaryValue<TValue>(IDictionary<string, TValue>? dictionary, string key, out TValue value)
    {
        value = default!;
        if (dictionary == null)
        {
            return false;
        }

        if (dictionary.TryGetValue(key, out var foundValue))
        {
            value = foundValue;
            return true;
        }

        foreach (var pair in dictionary)
        {
            if (string.Equals(pair.Key, key, StringComparison.OrdinalIgnoreCase))
            {
                value = pair.Value;
                return true;
            }
        }

        return false;
    }

    private static string? FindExistingKey(IDictionary<string, object?> dictionary, string key)
    {
        if (dictionary.ContainsKey(key))
        {
            return key;
        }

        foreach (var pair in dictionary)
        {
            if (string.Equals(pair.Key, key, StringComparison.OrdinalIgnoreCase))
            {
                return pair.Key;
            }
        }

        return null;
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
        ValidateCommandText(query);
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

            object? result;
            using (var reader = command.ExecuteReader())
            {
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
                        result = row;
                    }
                    else
                    {
                        result = null;
                    }
                }
                else if (returnType == ReturnType.DataTable || returnType == ReturnType.PSObject)
                {
                    var table = new DataTable("Table0");
                    table.Load(reader);
                    result = table;
                }
                else
                {
                    var dataSet = new DataSet();
                    var tableIndex = 0;
                    do
                    {
                        var table = new DataTable($"Table{tableIndex}");
                        table.Load(reader);
                        dataSet.Tables.Add(table);
                        tableIndex++;
                    } while (!reader.IsClosed && reader.NextResult());

                    result = BuildResult(dataSet);
                }
            }

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
        ValidateCommandText(query);
        int ExecuteOperation()
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
        }

        return RetryNonQueryOperations
            ? ExecuteWithRetry(ExecuteOperation)
            : ExecuteOperation();
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
        ValidateCommandText(query);
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
        ValidateCommandText(query);
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

            object? result;
            using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
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
                        result = row;
                    }
                    else
                    {
                        result = null;
                    }
                }
                else if (returnType == ReturnType.DataTable || returnType == ReturnType.PSObject)
                {
                    var table = new DataTable("Table0");
                    table.Load(reader);
                    result = table;
                }
                else
                {
                    var dataSet = new DataSet();
                    var tableIndex = 0;
                    do
                    {
                        var table = new DataTable($"Table{tableIndex}");
                        table.Load(reader);
                        dataSet.Tables.Add(table);
                        tableIndex++;
                    } while (!reader.IsClosed && await reader.NextResultAsync(cancellationToken).ConfigureAwait(false));

                    result = BuildResult(dataSet);
                }
            }

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
        ValidateCommandText(query);
        async Task<int> ExecuteOperationAsync()
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
        }

        if (RetryNonQueryOperations)
        {
            return await ExecuteWithRetryAsync(ExecuteOperationAsync, cancellationToken).ConfigureAwait(false);
        }

        return await ExecuteOperationAsync().ConfigureAwait(false);
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
        ValidateCommandText(query);
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
        ValidateCommandText(query, commandType);
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
    /// Validates SQL text or stored procedure names passed to execution helpers.
    /// </summary>
    protected static void ValidateCommandText(string commandText, CommandType commandType = CommandType.Text)
    {
        if (!string.IsNullOrWhiteSpace(commandText))
        {
            return;
        }

        var parameterName = commandType == CommandType.StoredProcedure ? "procedure" : "query";
        var message = commandType == CommandType.StoredProcedure
            ? "Stored procedure name cannot be null or whitespace."
            : "Query text cannot be null or whitespace.";
        throw new ArgumentException(message, parameterName);
    }

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
    /// Attempts to roll back an active transaction after an operation failure and rethrows the original exception.
    /// </summary>
    protected static void HandleTransactionFailure(Exception originalException, Action rollback, Func<bool> hasActiveTransaction)
    {
        var hadActiveTransaction = hasActiveTransaction();
        Exception? rollbackException = null;
        try
        {
            rollback();
        }
        catch (DbaTransactionException) when (!hadActiveTransaction)
        {
            rollbackException = null;
        }
        catch (Exception ex)
        {
            rollbackException = ex;
        }

        if (rollbackException != null)
        {
            throw new AggregateException("Transaction operation failed and rollback also failed.", originalException, rollbackException);
        }

        ExceptionDispatchInfo.Capture(originalException).Throw();
    }

    /// <summary>
    /// Attempts to roll back an active transaction after an asynchronous operation failure and rethrows the original exception.
    /// </summary>
    protected static async Task HandleTransactionFailureAsync(
        Exception originalException,
        Func<CancellationToken, Task> rollbackAsync,
        Func<bool> hasActiveTransaction,
        CancellationToken cancellationToken)
    {
        var hadActiveTransaction = hasActiveTransaction();
        Exception? rollbackException = null;
        try
        {
            await rollbackAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (DbaTransactionException) when (!hadActiveTransaction)
        {
            rollbackException = null;
        }
        catch (Exception ex)
        {
            rollbackException = ex;
        }

        if (rollbackException != null)
        {
            throw new AggregateException("Transaction operation failed and rollback also failed.", originalException, rollbackException);
        }

        ExceptionDispatchInfo.Capture(originalException).Throw();
    }

    /// <summary>
    /// Executes an operation inside an already-configured transaction lifecycle.
    /// </summary>
    /// <typeparam name="TResult">The operation result type.</typeparam>
    /// <param name="beginTransaction">Callback that starts the transaction.</param>
    /// <param name="operation">Callback executed inside the transaction.</param>
    /// <param name="commitTransaction">Callback that commits the transaction.</param>
    /// <param name="rollbackTransaction">Callback that rolls back the transaction on failure.</param>
    /// <param name="hasActiveTransaction">Callback that indicates whether a transaction is still active.</param>
    protected static TResult ExecuteInTransaction<TResult>(
        Action beginTransaction,
        Func<TResult> operation,
        Action commitTransaction,
        Action rollbackTransaction,
        Func<bool> hasActiveTransaction)
    {
        beginTransaction();
        try
        {
            var result = operation();
            commitTransaction();
            return result;
        }
        catch (Exception ex)
        {
            HandleTransactionFailure(ex, rollbackTransaction, hasActiveTransaction);
            throw;
        }
    }

    /// <summary>
    /// Executes an asynchronous operation inside an already-configured transaction lifecycle.
    /// </summary>
    /// <typeparam name="TResult">The operation result type.</typeparam>
    /// <param name="beginTransactionAsync">Callback that starts the transaction.</param>
    /// <param name="operationAsync">Callback executed inside the transaction.</param>
    /// <param name="commitTransactionAsync">Callback that commits the transaction.</param>
    /// <param name="rollbackTransactionAsync">Callback that rolls back the transaction on failure.</param>
    /// <param name="hasActiveTransaction">Callback that indicates whether a transaction is still active.</param>
    /// <param name="cancellationToken">Cancellation token for the async workflow.</param>
    protected static async Task<TResult> ExecuteInTransactionAsync<TResult>(
        Func<CancellationToken, Task> beginTransactionAsync,
        Func<CancellationToken, Task<TResult>> operationAsync,
        Func<CancellationToken, Task> commitTransactionAsync,
        Func<CancellationToken, Task> rollbackTransactionAsync,
        Func<bool> hasActiveTransaction,
        CancellationToken cancellationToken)
    {
        await beginTransactionAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var result = await operationAsync(cancellationToken).ConfigureAwait(false);
            await commitTransactionAsync(cancellationToken).ConfigureAwait(false);
            return result;
        }
        catch (Exception ex)
        {
            await HandleTransactionFailureAsync(ex, rollbackTransactionAsync, hasActiveTransaction, cancellationToken).ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Disposes the instance and suppresses finalization.
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposeSignaled, 1) != 0)
        {
            return;
        }

        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Asynchronously disposes the instance and suppresses finalization.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeSignaled, 1) != 0)
        {
            return;
        }

        await DisposeAsyncCore().ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Releases managed resources asynchronously. Override to dispose async-aware state.
    /// </summary>
    protected virtual ValueTask DisposeAsyncCore()
    {
        Dispose(true);
        return default;
    }

    /// <summary>
    /// Releases managed resources. Override to dispose additional state.
    /// </summary>
    /// <param name="disposing">Indicates whether the method is invoked from <see cref="Dispose()"/>.</param>
    protected virtual void Dispose(bool disposing)
    {
    }
}
