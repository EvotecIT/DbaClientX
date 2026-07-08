using System;
using System.Data;
using System.Data.Common;

namespace DBAClientX;

/// <summary>
/// Wraps a provider reader together with the command and owned connection resources that must stay alive while it is consumed.
/// </summary>
public sealed class DbaDataReader : IDataReader
{
    private readonly IDataReader _reader;
    private readonly IDisposable? _command;
    private readonly DbConnection? _connection;
    private readonly bool _ownsConnection;
    private readonly Action<DbConnection>? _disposeConnection;
    private readonly Action? _afterReaderDisposed;
    private bool _disposed;

    /// <summary>
    /// Initializes a reader lease around an already-open provider reader.
    /// </summary>
    public DbaDataReader(
        IDataReader reader,
        IDisposable? command = null,
        DbConnection? connection = null,
        bool ownsConnection = false,
        Action<DbConnection>? disposeConnection = null,
        Action? afterReaderDisposed = null)
    {
        _reader = reader ?? throw new ArgumentNullException(nameof(reader));
        _command = command;
        _connection = connection;
        _ownsConnection = ownsConnection;
        _disposeConnection = disposeConnection;
        _afterReaderDisposed = afterReaderDisposed;
    }

    /// <inheritdoc />
    public int Depth => _reader.Depth;

    /// <inheritdoc />
    public bool IsClosed => _reader.IsClosed;

    /// <inheritdoc />
    public int RecordsAffected => _reader.RecordsAffected;

    /// <inheritdoc />
    public int FieldCount => _reader.FieldCount;

    /// <inheritdoc />
    public object this[int i] => _reader[i];

    /// <inheritdoc />
    public object this[string name] => _reader[name];

    /// <inheritdoc />
    public void Close() => Dispose();

    /// <inheritdoc />
    public DataTable? GetSchemaTable() => _reader.GetSchemaTable();

    /// <inheritdoc />
    public bool NextResult() => _reader.NextResult();

    /// <inheritdoc />
    public bool Read() => _reader.Read();

    /// <inheritdoc />
    public bool GetBoolean(int i) => _reader.GetBoolean(i);

    /// <inheritdoc />
    public byte GetByte(int i) => _reader.GetByte(i);

    /// <inheritdoc />
    public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length) => _reader.GetBytes(i, fieldOffset, buffer, bufferoffset, length);

    /// <inheritdoc />
    public char GetChar(int i) => _reader.GetChar(i);

    /// <inheritdoc />
    public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length) => _reader.GetChars(i, fieldoffset, buffer, bufferoffset, length);

    /// <inheritdoc />
    public IDataReader GetData(int i) => _reader.GetData(i);

    /// <inheritdoc />
    public string GetDataTypeName(int i) => _reader.GetDataTypeName(i);

    /// <inheritdoc />
    public DateTime GetDateTime(int i) => _reader.GetDateTime(i);

    /// <inheritdoc />
    public decimal GetDecimal(int i) => _reader.GetDecimal(i);

    /// <inheritdoc />
    public double GetDouble(int i) => _reader.GetDouble(i);

    /// <inheritdoc />
#pragma warning disable IL2093, IL2073
    public Type GetFieldType(int i) => _reader.GetFieldType(i);
#pragma warning restore IL2093, IL2073

    /// <inheritdoc />
    public float GetFloat(int i) => _reader.GetFloat(i);

    /// <inheritdoc />
    public Guid GetGuid(int i) => _reader.GetGuid(i);

    /// <inheritdoc />
    public short GetInt16(int i) => _reader.GetInt16(i);

    /// <inheritdoc />
    public int GetInt32(int i) => _reader.GetInt32(i);

    /// <inheritdoc />
    public long GetInt64(int i) => _reader.GetInt64(i);

    /// <inheritdoc />
    public string GetName(int i) => _reader.GetName(i);

    /// <inheritdoc />
    public int GetOrdinal(string name) => _reader.GetOrdinal(name);

    /// <inheritdoc />
    public string GetString(int i) => _reader.GetString(i);

    /// <inheritdoc />
    public object GetValue(int i) => _reader.GetValue(i);

    /// <inheritdoc />
    public int GetValues(object[] values) => _reader.GetValues(values);

    /// <inheritdoc />
    public bool IsDBNull(int i) => _reader.IsDBNull(i);

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
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
}
