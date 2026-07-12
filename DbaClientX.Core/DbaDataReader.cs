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
    public override bool HasRows => _dbReader?.HasRows
        ?? throw new NotSupportedException("HasRows is unavailable when the wrapped reader is not a DbDataReader.");

    /// <inheritdoc />
    public override object this[int ordinal] => _reader[ordinal];

    /// <inheritdoc />
    public override object this[string name] => _reader[name];

    /// <inheritdoc />
    public override void Close() => Dispose();

    /// <inheritdoc />
    public override DataTable? GetSchemaTable() => _reader.GetSchemaTable();

    /// <inheritdoc />
    public override bool NextResult() => _reader.NextResult();

    /// <inheritdoc />
    public override bool Read() => _reader.Read();

    /// <inheritdoc />
    public override bool GetBoolean(int ordinal) => _reader.GetBoolean(ordinal);

    /// <inheritdoc />
    public override byte GetByte(int ordinal) => _reader.GetByte(ordinal);

    /// <inheritdoc />
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        => _reader.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);

    /// <inheritdoc />
    public override char GetChar(int ordinal) => _reader.GetChar(ordinal);

    /// <inheritdoc />
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        => _reader.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);

    /// <inheritdoc />
    public override string GetDataTypeName(int ordinal) => _reader.GetDataTypeName(ordinal);

    /// <inheritdoc />
    public override DateTime GetDateTime(int ordinal) => _reader.GetDateTime(ordinal);

    /// <inheritdoc />
    public override decimal GetDecimal(int ordinal) => _reader.GetDecimal(ordinal);

    /// <inheritdoc />
    public override double GetDouble(int ordinal) => _reader.GetDouble(ordinal);

    /// <inheritdoc />
#pragma warning disable IL2093, IL2073
    public override Type GetFieldType(int ordinal) => _reader.GetFieldType(ordinal);
#pragma warning restore IL2093, IL2073

    /// <inheritdoc />
    public override float GetFloat(int ordinal) => _reader.GetFloat(ordinal);

    /// <inheritdoc />
    public override Guid GetGuid(int ordinal) => _reader.GetGuid(ordinal);

    /// <inheritdoc />
    public override short GetInt16(int ordinal) => _reader.GetInt16(ordinal);

    /// <inheritdoc />
    public override int GetInt32(int ordinal) => _reader.GetInt32(ordinal);

    /// <inheritdoc />
    public override long GetInt64(int ordinal) => _reader.GetInt64(ordinal);

    /// <inheritdoc />
    public override string GetName(int ordinal) => _reader.GetName(ordinal);

    /// <inheritdoc />
    public override int GetOrdinal(string name) => _reader.GetOrdinal(name);

    /// <inheritdoc />
    public override string GetString(int ordinal) => _reader.GetString(ordinal);

    /// <inheritdoc />
    public override object GetValue(int ordinal) => _reader.GetValue(ordinal);

    /// <inheritdoc />
    public override int GetValues(object[] values) => _reader.GetValues(values);

    /// <inheritdoc />
    public override bool IsDBNull(int ordinal) => _reader.IsDBNull(ordinal);

    /// <inheritdoc />
    protected override DbDataReader GetDbDataReader(int ordinal)
    {
        var nestedReader = _reader.GetData(ordinal);
        return nestedReader as DbDataReader
            ?? throw new NotSupportedException("The nested reader is not a DbDataReader.");
    }

    /// <inheritdoc />
    public override IEnumerator GetEnumerator() => _dbReader?.GetEnumerator() ?? new DbEnumerator(this, closeReader: false);

    /// <inheritdoc />
    public override T GetFieldValue<T>(int ordinal)
        => _dbReader != null ? _dbReader.GetFieldValue<T>(ordinal) : (T)GetValue(ordinal);

    /// <inheritdoc />
    public override Stream GetStream(int ordinal)
        => _dbReader != null ? _dbReader.GetStream(ordinal) : base.GetStream(ordinal);

    /// <inheritdoc />
    public override TextReader GetTextReader(int ordinal)
        => _dbReader != null ? _dbReader.GetTextReader(ordinal) : base.GetTextReader(ordinal);

    /// <inheritdoc />
    public override Task<bool> ReadAsync(CancellationToken cancellationToken)
        => _dbReader?.ReadAsync(cancellationToken) ?? base.ReadAsync(cancellationToken);

    /// <inheritdoc />
    public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
        => _dbReader?.NextResultAsync(cancellationToken) ?? base.NextResultAsync(cancellationToken);

    /// <inheritdoc />
    public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken)
        => _dbReader?.IsDBNullAsync(ordinal, cancellationToken) ?? base.IsDBNullAsync(ordinal, cancellationToken);

    /// <inheritdoc />
    public override Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken)
        => _dbReader?.GetFieldValueAsync<T>(ordinal, cancellationToken) ?? base.GetFieldValueAsync<T>(ordinal, cancellationToken);

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
