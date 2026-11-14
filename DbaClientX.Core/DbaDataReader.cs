using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace DBAClientX;

/// <summary>
/// Owns a provider reader together with the command and connection resources that must remain alive while rows are consumed.
/// </summary>
/// <remarks>
/// Both synchronous and asynchronous disposal are idempotent. Asynchronous disposal uses provider async cleanup when available
/// and still invokes the post-reader callback before the command and owned connection are released.
/// </remarks>
public sealed partial class DbaDataReader : DbDataReader
{
    private readonly IDataReader _reader;
    private readonly DbDataReader? _dbReader;
    private readonly IDisposable? _command;
    private readonly DbConnection? _connection;
    private readonly bool _ownsConnection;
    private readonly Action<DbConnection>? _disposeConnection;
    private readonly Func<DbConnection, ValueTask>? _disposeConnectionAsync;
    private readonly Action? _afterReaderDisposed;
    private readonly Func<ValueTask>? _afterReaderDisposedAsync;
    private readonly Func<Exception, CancellationToken, Exception>? _consumptionExceptionFactory;
    private int _disposeState;

    // Provider-owned consumers can retain native value/streaming semantics while this lease still owns cleanup.
    internal IDataReader ProviderReader => _reader;

    /// <summary>Initializes a reader lease around an already-open provider reader.</summary>
    /// <param name="reader">Provider reader that supplies the rows.</param>
    /// <param name="command">Optional command owned by this lease.</param>
    /// <param name="connection">Optional connection associated with the reader.</param>
    /// <param name="ownsConnection">Whether this lease must dispose <paramref name="connection"/>.</param>
    /// <param name="disposeConnection">Optional provider-specific synchronous connection disposer.</param>
    /// <param name="afterReaderDisposed">Optional synchronous callback invoked after the reader closes and before the command is disposed.</param>
    /// <param name="disposeConnectionAsync">Optional provider-specific asynchronous connection disposer.</param>
    /// <param name="afterReaderDisposedAsync">Optional asynchronous callback invoked after the reader closes and before the command is disposed.</param>
    public DbaDataReader(
        IDataReader reader,
        IDisposable? command = null,
        DbConnection? connection = null,
        bool ownsConnection = false,
        Action<DbConnection>? disposeConnection = null,
        Action? afterReaderDisposed = null,
        Func<DbConnection, ValueTask>? disposeConnectionAsync = null,
        Func<ValueTask>? afterReaderDisposedAsync = null)
        : this(
            reader,
            command,
            connection,
            ownsConnection,
            disposeConnection,
            afterReaderDisposed,
            disposeConnectionAsync,
            afterReaderDisposedAsync,
            consumptionExceptionFactory: null)
    {
    }

    /// <summary>Initializes a reader lease that also normalizes provider failures raised while rows are consumed.</summary>
    /// <param name="reader">Provider reader that supplies the rows.</param>
    /// <param name="command">Optional command owned by this lease.</param>
    /// <param name="connection">Optional connection associated with the reader.</param>
    /// <param name="ownsConnection">Whether this lease must dispose <paramref name="connection"/>.</param>
    /// <param name="disposeConnection">Optional provider-specific synchronous connection disposer.</param>
    /// <param name="afterReaderDisposed">Optional synchronous callback invoked after the reader closes and before the command is disposed.</param>
    /// <param name="disposeConnectionAsync">Optional provider-specific asynchronous connection disposer.</param>
    /// <param name="afterReaderDisposedAsync">Optional asynchronous callback invoked after the reader closes and before the command is disposed.</param>
    /// <param name="consumptionExceptionFactory">Maps deferred provider failures to the library's sanitized public exception contract.</param>
    public DbaDataReader(
        IDataReader reader,
        IDisposable? command,
        DbConnection? connection,
        bool ownsConnection,
        Action<DbConnection>? disposeConnection,
        Action? afterReaderDisposed,
        Func<DbConnection, ValueTask>? disposeConnectionAsync,
        Func<ValueTask>? afterReaderDisposedAsync,
        Func<Exception, CancellationToken, Exception>? consumptionExceptionFactory)
    {
        _reader = reader ?? throw new ArgumentNullException(nameof(reader));
        _dbReader = reader as DbDataReader;
        _command = command;
        _connection = connection;
        _ownsConnection = ownsConnection;
        _disposeConnection = disposeConnection;
        _afterReaderDisposed = afterReaderDisposed;
        _disposeConnectionAsync = disposeConnectionAsync;
        _afterReaderDisposedAsync = afterReaderDisposedAsync;
        _consumptionExceptionFactory = consumptionExceptionFactory;
    }

    /// <inheritdoc />
    public override int Depth => _reader.Depth;

    /// <inheritdoc />
    public override bool IsClosed => _reader.IsClosed;

    /// <inheritdoc />
    public override int RecordsAffected => _reader.RecordsAffected;

    /// <inheritdoc />
    public override int FieldCount => _reader.FieldCount;

    /// <inheritdoc />
    public override int VisibleFieldCount => _dbReader?.VisibleFieldCount ?? _reader.FieldCount;

    /// <inheritdoc />
    public override bool HasRows => ExecuteConsumptionOperation(
        () => _dbReader?.HasRows
            ?? throw new NotSupportedException("HasRows is unavailable when the wrapped reader is not a DbDataReader."),
        CancellationToken.None);

    /// <inheritdoc />
    public override object this[int ordinal] => Consume(ordinal, static (reader, index) => reader[index]);

    /// <inheritdoc />
    public override object this[string name] => Consume(name, static (reader, key) => reader[key]);

    /// <inheritdoc />
    public override void Close() => Dispose();

    /// <inheritdoc />
    public override DataTable? GetSchemaTable()
        => ExecuteConsumptionOperation(_reader.GetSchemaTable, CancellationToken.None);

    /// <inheritdoc />
    public override bool NextResult() => Consume(0, static (reader, _) => reader.NextResult());

    /// <inheritdoc />
    public override bool Read() => Consume(0, static (reader, _) => reader.Read());

    /// <inheritdoc />
    public override bool GetBoolean(int ordinal)
        => Consume(ordinal, static (reader, index) => reader.GetBoolean(index));

    /// <inheritdoc />
    public override byte GetByte(int ordinal)
        => Consume(ordinal, static (reader, index) => reader.GetByte(index));

    /// <inheritdoc />
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        => ExecuteConsumptionOperation(
            () => _reader.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length),
            CancellationToken.None);

    /// <inheritdoc />
    public override char GetChar(int ordinal)
        => Consume(ordinal, static (reader, index) => reader.GetChar(index));

    /// <inheritdoc />
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        => ExecuteConsumptionOperation(
            () => _reader.GetChars(ordinal, dataOffset, buffer, bufferOffset, length),
            CancellationToken.None);

    /// <inheritdoc />
    public override string GetDataTypeName(int ordinal)
        => Consume(ordinal, static (reader, index) => reader.GetDataTypeName(index));

    /// <inheritdoc />
    public override DateTime GetDateTime(int ordinal)
        => Consume(ordinal, static (reader, index) => reader.GetDateTime(index));

    /// <inheritdoc />
    public override decimal GetDecimal(int ordinal)
        => Consume(ordinal, static (reader, index) => reader.GetDecimal(index));

    /// <inheritdoc />
    public override double GetDouble(int ordinal)
        => Consume(ordinal, static (reader, index) => reader.GetDouble(index));

    /// <inheritdoc />
#if NET8_0_OR_GREATER
    [return: System.Diagnostics.CodeAnalysis.DynamicallyAccessedMembers(
        System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicFields |
        System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicProperties)]
#endif
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2073",
        Justification = "The wrapped DbDataReader supplies the annotated field type; the consumption delegate only forwards that value.")]
    public override Type GetFieldType(int ordinal)
        => Consume(ordinal, static (reader, index) => reader.GetFieldType(index));

    /// <inheritdoc />
    public override float GetFloat(int ordinal)
        => Consume(ordinal, static (reader, index) => reader.GetFloat(index));

    /// <inheritdoc />
    public override Guid GetGuid(int ordinal)
        => Consume(ordinal, static (reader, index) => reader.GetGuid(index));

    /// <inheritdoc />
    public override short GetInt16(int ordinal)
        => Consume(ordinal, static (reader, index) => reader.GetInt16(index));

    /// <inheritdoc />
    public override int GetInt32(int ordinal)
        => Consume(ordinal, static (reader, index) => reader.GetInt32(index));

    /// <inheritdoc />
    public override long GetInt64(int ordinal)
        => Consume(ordinal, static (reader, index) => reader.GetInt64(index));

    /// <inheritdoc />
    public override string GetName(int ordinal)
        => Consume(ordinal, static (reader, index) => reader.GetName(index));

    /// <inheritdoc />
    public override int GetOrdinal(string name)
        => Consume(name, static (reader, key) => reader.GetOrdinal(key));

    /// <inheritdoc />
    public override string GetString(int ordinal)
        => Consume(ordinal, static (reader, index) => reader.GetString(index));

    /// <inheritdoc />
    public override object GetValue(int ordinal)
        => Consume(ordinal, static (reader, index) => reader.GetValue(index));

    /// <inheritdoc />
    public override int GetValues(object[] values)
        => Consume(values, static (reader, buffer) => reader.GetValues(buffer));

    /// <inheritdoc />
    public override bool IsDBNull(int ordinal)
        => Consume(ordinal, static (reader, index) => reader.IsDBNull(index));

    /// <inheritdoc />
    protected override DbDataReader GetDbDataReader(int ordinal)
    {
        var nestedReader = Consume(ordinal, static (reader, index) => reader.GetData(index));
        if (nestedReader is not DbDataReader dbDataReader)
        {
            throw new NotSupportedException("The nested reader is not a DbDataReader.");
        }

        return _consumptionExceptionFactory == null
            ? dbDataReader
            : new DbaDataReader(
                dbDataReader,
                command: null,
                connection: null,
                ownsConnection: false,
                disposeConnection: null,
                afterReaderDisposed: null,
                disposeConnectionAsync: null,
                afterReaderDisposedAsync: null,
                consumptionExceptionFactory: _consumptionExceptionFactory);
    }

    /// <inheritdoc />
    public override IEnumerator GetEnumerator() => new DbEnumerator(this, closeReader: false);

    /// <inheritdoc />
    public override T GetFieldValue<T>(int ordinal)
        => _dbReader != null
            ? ConsumeFieldValue<T>(_dbReader, ordinal)
            : (T)GetValue(ordinal);

    /// <inheritdoc />
    public override Stream GetStream(int ordinal)
    {
        Stream stream = _dbReader != null
            ? ExecuteConsumptionOperation(() => _dbReader.GetStream(ordinal), CancellationToken.None)
            : base.GetStream(ordinal);
        return _consumptionExceptionFactory == null
            ? stream
            : new NormalizingReadStream(stream, _consumptionExceptionFactory);
    }

    /// <inheritdoc />
    public override TextReader GetTextReader(int ordinal)
    {
        TextReader reader = _dbReader != null
            ? ExecuteConsumptionOperation(() => _dbReader.GetTextReader(ordinal), CancellationToken.None)
            : base.GetTextReader(ordinal);
        return _consumptionExceptionFactory == null
            ? reader
            : new NormalizingTextReader(reader, _consumptionExceptionFactory);
    }

    /// <inheritdoc />
    public override Task<bool> ReadAsync(CancellationToken cancellationToken)
        => ExecuteConsumptionOperationAsync(
            () => _dbReader?.ReadAsync(cancellationToken) ?? base.ReadAsync(cancellationToken),
            cancellationToken);

    /// <inheritdoc />
    public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
        => ExecuteConsumptionOperationAsync(
            () => _dbReader?.NextResultAsync(cancellationToken) ?? base.NextResultAsync(cancellationToken),
            cancellationToken);

    /// <inheritdoc />
    public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken)
        => ExecuteConsumptionOperationAsync(
            () => _dbReader?.IsDBNullAsync(ordinal, cancellationToken) ?? base.IsDBNullAsync(ordinal, cancellationToken),
            cancellationToken);

    /// <inheritdoc />
    public override Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken)
        => ExecuteConsumptionOperationAsync(
            () => _dbReader?.GetFieldValueAsync<T>(ordinal, cancellationToken) ?? base.GetFieldValueAsync<T>(ordinal, cancellationToken),
            cancellationToken);

    private T Consume<TArgument, T>(TArgument argument, Func<IDataReader, TArgument, T> operation)
    {
        // Row and value accessors run per field; cached static delegates keep them allocation-free.
        try
        {
            return operation(_reader, argument);
        }
        catch (Exception exception) when (ShouldNormalizeConsumptionException(exception, CancellationToken.None))
        {
            throw _consumptionExceptionFactory!(exception, CancellationToken.None);
        }
    }

    private T ConsumeFieldValue<T>(DbDataReader reader, int ordinal)
    {
        try
        {
            return reader.GetFieldValue<T>(ordinal);
        }
        catch (Exception exception) when (ShouldNormalizeConsumptionException(exception, CancellationToken.None))
        {
            throw _consumptionExceptionFactory!(exception, CancellationToken.None);
        }
    }

    private T ExecuteConsumptionOperation<T>(Func<T> operation, CancellationToken cancellationToken)
    {
        try
        {
            return operation();
        }
        catch (Exception exception) when (ShouldNormalizeConsumptionException(exception, cancellationToken))
        {
            throw _consumptionExceptionFactory!(exception, cancellationToken);
        }
    }

    private async Task<T> ExecuteConsumptionOperationAsync<T>(Func<Task<T>> operation, CancellationToken cancellationToken)
    {
        try
        {
            return await operation().ConfigureAwait(false);
        }
        catch (Exception exception) when (ShouldNormalizeConsumptionException(exception, cancellationToken))
        {
            throw _consumptionExceptionFactory!(exception, cancellationToken);
        }
    }

    private bool ShouldNormalizeConsumptionException(Exception exception, CancellationToken cancellationToken)
        => _consumptionExceptionFactory != null && IsProviderConsumptionException(exception, cancellationToken);

    private static bool IsProviderConsumptionException(Exception exception, CancellationToken cancellationToken)
        => (exception is DbException or IOException or TimeoutException or OperationCanceledException) &&
           !IsCallerCancellation(exception, cancellationToken);

    private static bool IsCallerCancellation(Exception exception, CancellationToken cancellationToken)
        => exception is OperationCanceledException &&
           cancellationToken.CanBeCanceled &&
           cancellationToken.IsCancellationRequested;

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            DisposeOwnedResources();
        }
    }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
    /// <inheritdoc />
    public override ValueTask DisposeAsync() => DisposeOwnedResourcesAsync();
#else
    /// <summary>Asynchronously disposes the reader lease and its owned resources.</summary>
    public ValueTask DisposeAsync() => DisposeOwnedResourcesAsync();
#endif

    private void DisposeOwnedResources()
    {
        if (Interlocked.Exchange(ref _disposeState, 1) != 0)
        {
            return;
        }

        try
        {
            try
            {
                _reader.Dispose();
                _afterReaderDisposed?.Invoke();
            }
            finally
            {
                _command?.Dispose();
            }
        }
        finally
        {
            if (_ownsConnection && _connection != null)
            {
                if (_disposeConnection != null)
                {
                    _disposeConnection(_connection);
                }
                else
                {
                    _connection.Dispose();
                }
            }
        }
    }

    private async ValueTask DisposeOwnedResourcesAsync()
    {
        if (Interlocked.Exchange(ref _disposeState, 1) != 0)
        {
            return;
        }

        try
        {
            try
            {
                await DisposeAsyncResource(_reader).ConfigureAwait(false);
                if (_afterReaderDisposedAsync != null)
                {
                    await _afterReaderDisposedAsync().ConfigureAwait(false);
                }
                else
                {
                    _afterReaderDisposed?.Invoke();
                }
            }
            finally
            {
                await DisposeAsyncResource(_command).ConfigureAwait(false);
            }
        }
        finally
        {
            if (_ownsConnection && _connection != null)
            {
                if (_disposeConnectionAsync != null)
                {
                    await _disposeConnectionAsync(_connection).ConfigureAwait(false);
                }
                else if (_connection is IAsyncDisposable asyncConnection)
                {
                    await asyncConnection.DisposeAsync().ConfigureAwait(false);
                }
                else if (_disposeConnection != null)
                {
                    _disposeConnection(_connection);
                }
                else
                {
                    _connection.Dispose();
                }
            }
        }
    }

    private static async ValueTask DisposeAsyncResource(object? resource)
    {
        if (resource is IAsyncDisposable asyncDisposable)
        {
            await asyncDisposable.DisposeAsync().ConfigureAwait(false);
        }
        else if (resource is IDisposable disposable)
        {
            disposable.Dispose();
        }
    }
}
