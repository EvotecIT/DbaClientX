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
public sealed class DbaDataReader : DbDataReader
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
    public override object this[int ordinal] => ExecuteConsumptionOperation(
        () => _reader[ordinal],
        CancellationToken.None);

    /// <inheritdoc />
    public override object this[string name] => ExecuteConsumptionOperation(
        () => _reader[name],
        CancellationToken.None);

    /// <inheritdoc />
    public override void Close() => Dispose();

    /// <inheritdoc />
    public override DataTable? GetSchemaTable()
        => ExecuteConsumptionOperation(_reader.GetSchemaTable, CancellationToken.None);

    /// <inheritdoc />
    public override bool NextResult() => ExecuteConsumptionOperation(_reader.NextResult, CancellationToken.None);

    /// <inheritdoc />
    public override bool Read() => ExecuteConsumptionOperation(_reader.Read, CancellationToken.None);

    /// <inheritdoc />
    public override bool GetBoolean(int ordinal)
        => ExecuteConsumptionOperation(() => _reader.GetBoolean(ordinal), CancellationToken.None);

    /// <inheritdoc />
    public override byte GetByte(int ordinal)
        => ExecuteConsumptionOperation(() => _reader.GetByte(ordinal), CancellationToken.None);

    /// <inheritdoc />
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        => ExecuteConsumptionOperation(
            () => _reader.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length),
            CancellationToken.None);

    /// <inheritdoc />
    public override char GetChar(int ordinal)
        => ExecuteConsumptionOperation(() => _reader.GetChar(ordinal), CancellationToken.None);

    /// <inheritdoc />
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        => ExecuteConsumptionOperation(
            () => _reader.GetChars(ordinal, dataOffset, buffer, bufferOffset, length),
            CancellationToken.None);

    /// <inheritdoc />
    public override string GetDataTypeName(int ordinal)
        => ExecuteConsumptionOperation(() => _reader.GetDataTypeName(ordinal), CancellationToken.None);

    /// <inheritdoc />
    public override DateTime GetDateTime(int ordinal)
        => ExecuteConsumptionOperation(() => _reader.GetDateTime(ordinal), CancellationToken.None);

    /// <inheritdoc />
    public override decimal GetDecimal(int ordinal)
        => ExecuteConsumptionOperation(() => _reader.GetDecimal(ordinal), CancellationToken.None);

    /// <inheritdoc />
    public override double GetDouble(int ordinal)
        => ExecuteConsumptionOperation(() => _reader.GetDouble(ordinal), CancellationToken.None);

    /// <inheritdoc />
#pragma warning disable IL2093, IL2073
    public override Type GetFieldType(int ordinal)
        => ExecuteConsumptionOperation(() => _reader.GetFieldType(ordinal), CancellationToken.None);
#pragma warning restore IL2093, IL2073

    /// <inheritdoc />
    public override float GetFloat(int ordinal)
        => ExecuteConsumptionOperation(() => _reader.GetFloat(ordinal), CancellationToken.None);

    /// <inheritdoc />
    public override Guid GetGuid(int ordinal)
        => ExecuteConsumptionOperation(() => _reader.GetGuid(ordinal), CancellationToken.None);

    /// <inheritdoc />
    public override short GetInt16(int ordinal)
        => ExecuteConsumptionOperation(() => _reader.GetInt16(ordinal), CancellationToken.None);

    /// <inheritdoc />
    public override int GetInt32(int ordinal)
        => ExecuteConsumptionOperation(() => _reader.GetInt32(ordinal), CancellationToken.None);

    /// <inheritdoc />
    public override long GetInt64(int ordinal)
        => ExecuteConsumptionOperation(() => _reader.GetInt64(ordinal), CancellationToken.None);

    /// <inheritdoc />
    public override string GetName(int ordinal)
        => ExecuteConsumptionOperation(() => _reader.GetName(ordinal), CancellationToken.None);

    /// <inheritdoc />
    public override int GetOrdinal(string name)
        => ExecuteConsumptionOperation(() => _reader.GetOrdinal(name), CancellationToken.None);

    /// <inheritdoc />
    public override string GetString(int ordinal)
        => ExecuteConsumptionOperation(() => _reader.GetString(ordinal), CancellationToken.None);

    /// <inheritdoc />
    public override object GetValue(int ordinal)
        => ExecuteConsumptionOperation(() => _reader.GetValue(ordinal), CancellationToken.None);

    /// <inheritdoc />
    public override int GetValues(object[] values)
        => ExecuteConsumptionOperation(() => _reader.GetValues(values), CancellationToken.None);

    /// <inheritdoc />
    public override bool IsDBNull(int ordinal)
        => ExecuteConsumptionOperation(() => _reader.IsDBNull(ordinal), CancellationToken.None);

    /// <inheritdoc />
    protected override DbDataReader GetDbDataReader(int ordinal)
    {
        var nestedReader = ExecuteConsumptionOperation(() => _reader.GetData(ordinal), CancellationToken.None);
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
            ? ExecuteConsumptionOperation(() => _dbReader.GetFieldValue<T>(ordinal), CancellationToken.None)
            : (T)GetValue(ordinal);

    /// <inheritdoc />
    public override Stream GetStream(int ordinal)
        => _dbReader != null
            ? ExecuteConsumptionOperation(() => _dbReader.GetStream(ordinal), CancellationToken.None)
            : base.GetStream(ordinal);

    /// <inheritdoc />
    public override TextReader GetTextReader(int ordinal)
        => _dbReader != null
            ? ExecuteConsumptionOperation(() => _dbReader.GetTextReader(ordinal), CancellationToken.None)
            : base.GetTextReader(ordinal);

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

    private T ExecuteConsumptionOperation<T>(Func<T> operation, CancellationToken cancellationToken)
    {
        try
        {
            return operation();
        }
        catch (Exception exception) when (_consumptionExceptionFactory != null)
        {
            throw _consumptionExceptionFactory(exception, cancellationToken);
        }
    }

    private async Task<T> ExecuteConsumptionOperationAsync<T>(Func<Task<T>> operation, CancellationToken cancellationToken)
    {
        try
        {
            return await operation().ConfigureAwait(false);
        }
        catch (Exception exception) when (_consumptionExceptionFactory != null)
        {
            throw _consumptionExceptionFactory(exception, cancellationToken);
        }
    }

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
