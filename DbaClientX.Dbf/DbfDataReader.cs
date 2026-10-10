using System.Collections;
using System.Data;
using System.Data.Common;
using DBAClientX.Dbf.Internal;

namespace DBAClientX.Dbf;

/// <summary>Reads one DBF table through standard ADO.NET schema and row APIs without a database driver.</summary>
/// <remarks>Reads are forward-only; memo values use bounded random access to an optional seekable sidecar. Caller streams
/// start at their current positions. This reader is not thread-safe. Indexes, backlinks and embedded objects are never activated.</remarks>
public sealed partial class DbfDataReader : DbDataReader
{
    private readonly Stream _table;
    private readonly DbfReadOptions _options;
    private readonly DbfHeaderReader _header;
    private readonly CancellationToken _cancellationToken;
    private readonly bool _ownsTable;
    private readonly byte[] _record;
    private readonly object?[] _values;
    private readonly Dictionary<string, int> _ordinals;
    private DbfMemoReader? _memo;
    private bool _ownsMemo;
    private bool _closed, _faulted, _current, _prefetched;
    private bool? _hasRows;
    private long _physicalRecords, _currentRecordNumber;

    private DbfDataReader(Stream table, DbfReadOptions options, DbfHeaderReader header, bool ownsTable, CancellationToken cancellationToken)
    {
        _table = table; _options = options; _header = header; _ownsTable = ownsTable;
        _cancellationToken = cancellationToken;
        _record = new byte[header.Schema.RecordLength];
        _values = new object?[header.Schema.Columns.Count];
        _ordinals = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (int ordinal = 0; ordinal < header.Schema.Columns.Count; ordinal++)
            _ordinals.Add(header.Schema.Columns[ordinal].Name, ordinal);
    }

    /// <summary>Gets the decoded native schema independently of row traversal.</summary>
    public DbfTableSchema Schema => _header.Schema;

    /// <summary>Gets the current one-based physical record number, including skipped deleted records.</summary>
    public long SourceRecordNumber { get { RequireCurrent(); return _currentRecordNumber; } }

    /// <summary>Gets whether the current record carries the native deleted marker.</summary>
    public bool IsDeleted { get { RequireCurrent(); return _record[0] == 0x2a; } }

    /// <inheritdoc />
    public override int FieldCount => Schema.Columns.Count;
    /// <inheritdoc />
    public override int Depth => 0;
    /// <inheritdoc />
    public override bool IsClosed => _closed;
    /// <inheritdoc />
    public override int RecordsAffected => -1;
    /// <inheritdoc />
    public override object this[int ordinal] => GetValue(ordinal);
    /// <inheritdoc />
    public override object this[string name] => GetValue(GetOrdinal(name));

    /// <inheritdoc />
    public override bool HasRows
    {
        get
        {
            CheckState();
            if (_hasRows.HasValue) return _hasRows.Value;
            try
            {
                _prefetched = ReadAcceptedRecord(_cancellationToken);
                _hasRows = _prefetched;
                return _prefetched;
            }
            catch { _faulted = true; throw; }
        }
    }

    /// <inheritdoc />
    public override bool Read()
    {
        CheckState();
        BeginRecord();
        try
        {
            bool available = _prefetched || ReadAcceptedRecord(_cancellationToken);
            return AcceptRecord(available);
        }
        catch { _faulted = true; throw; }
    }

    /// <inheritdoc />
    public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
    {
        CheckState();
        cancellationToken.ThrowIfCancellationRequested();
        BeginRecord();
        using CancellationTokenSource? linked = cancellationToken.CanBeCanceled && _cancellationToken.CanBeCanceled
            ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cancellationToken) : null;
        CancellationToken token = linked?.Token ?? (cancellationToken.CanBeCanceled ? cancellationToken : _cancellationToken);
        try
        {
            bool available = _prefetched || await ReadAcceptedRecordAsync(token).ConfigureAwait(false);
            return AcceptRecord(available);
        }
        catch { _faulted = true; throw; }
    }

    private void BeginRecord()
    {
        _current = false;
        Array.Clear(_values, 0, _values.Length);
    }

    private bool AcceptRecord(bool available)
    {
        _prefetched = false;
        _current = available;
        if (available) { _currentRecordNumber = _physicalRecords; _hasRows = true; }
        else _hasRows ??= false;
        return available;
    }

    private bool ReadAcceptedRecord(CancellationToken token)
    {
        while (_physicalRecords < Schema.RecordCount)
        {
            DbfIO.ReadExactly(_table, _record, 0, _record.Length, token);
            _physicalRecords++;
            if (IsAcceptedRecord()) return true;
        }
        token.ThrowIfCancellationRequested();
        return false;
    }

    private async Task<bool> ReadAcceptedRecordAsync(CancellationToken token)
    {
        while (_physicalRecords < Schema.RecordCount)
        {
            await DbfIO.ReadExactlyAsync(_table, _record, 0, _record.Length, token).ConfigureAwait(false);
            _physicalRecords++;
            if (IsAcceptedRecord()) return true;
        }
        token.ThrowIfCancellationRequested();
        return false;
    }

    private bool IsAcceptedRecord()
    {
        if (_record[0] is not (0x20 or 0x2a)) throw new InvalidDataException("DBF record deletion marker is invalid.");
        return _record[0] == 0x20 || _options.IncludeDeletedRecords;
    }

    /// <inheritdoc />
    public override bool NextResult() { CheckState(); BeginRecord(); _prefetched = false; _physicalRecords = Schema.RecordCount; return false; }

    /// <inheritdoc />
    public override void Close() => Dispose();

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (!_closed && disposing)
        {
            _closed = true; _current = false;
            Array.Clear(_values, 0, _values.Length);
            try { if (_ownsMemo) _memo?.Dispose(); }
            finally { if (_ownsTable) _table.Dispose(); }
        }
        // DbDataReader.Dispose(bool) calls Close(); our Close routes here, so do not call it recursively.
    }

    private void CheckState()
    {
        if (_closed) throw new ObjectDisposedException(nameof(DbfDataReader));
        _cancellationToken.ThrowIfCancellationRequested();
        if (_faulted) throw new InvalidOperationException("The DBF reader cannot continue after a failed storage or value read.");
    }

    private void RequireCurrent()
    {
        CheckState();
        if (!_current) throw new InvalidOperationException("Read must select a record before accessing values.");
    }

    private DbfColumn Column(int ordinal)
    {
        if ((uint)ordinal >= (uint)FieldCount) throw new IndexOutOfRangeException("DBF column ordinal is out of range.");
        return Schema.Columns[ordinal];
    }

    /// <inheritdoc />
    public override string GetName(int ordinal) => Column(ordinal).Name;
    /// <inheritdoc />
    public override Type GetFieldType(int ordinal) => Column(ordinal).DataType;
    /// <inheritdoc />
    public override string GetDataTypeName(int ordinal) => Column(ordinal).NativeType.ToString();
    /// <inheritdoc />
    public override int GetOrdinal(string name) => name != null && _ordinals.TryGetValue(name, out int ordinal)
        ? ordinal : throw new IndexOutOfRangeException("DBF column name was not found.");
    /// <inheritdoc />
    public override IEnumerator GetEnumerator() => new DbEnumerator(this, closeReader: false);

    /// <inheritdoc />
    public override DataTable GetSchemaTable()
    {
        CheckState();
        var table = new DataTable("DbfSchema");
        table.Columns.Add("ColumnName", typeof(string)); table.Columns.Add("ColumnOrdinal", typeof(int));
        table.Columns.Add("ColumnSize", typeof(int)); table.Columns.Add("DataType", typeof(Type));
        table.Columns.Add("AllowDBNull", typeof(bool)); table.Columns.Add("IsLong", typeof(bool));
        table.Columns.Add("IsReadOnly", typeof(bool)); table.Columns.Add("IsUnique", typeof(bool));
        table.Columns.Add("IsKey", typeof(bool)); table.Columns.Add("NumericPrecision", typeof(short));
        table.Columns.Add("NumericScale", typeof(short)); table.Columns.Add("BaseColumnName", typeof(string));
        for (int ordinal = 0; ordinal < FieldCount; ordinal++)
        {
            DbfColumn column = Column(ordinal);
            DataRow row = table.NewRow();
            row["ColumnName"] = row["BaseColumnName"] = column.Name; row["ColumnOrdinal"] = ordinal;
            row["ColumnSize"] = column.IsMemo ? _options.MaxMemoBytes : column.Length;
            row["DataType"] = column.DataType; row["AllowDBNull"] = true; row["IsLong"] = column.IsMemo;
            row["IsReadOnly"] = true; row["IsUnique"] = false; row["IsKey"] = false;
            if (column.NativeType is 'N' or 'F')
            {
                row["NumericPrecision"] = (short)column.Length;
                row["NumericScale"] = (short)column.DecimalCount;
            }
            else if (column.NativeType == 'Y')
            {
                row["NumericPrecision"] = (short)19;
                row["NumericScale"] = (short)4;
            }
            table.Rows.Add(row);
        }
        return table;
    }
}
