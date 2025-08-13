using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using DBAClientX;
using Xunit;

namespace DbaClientX.Tests;

public class QueryStreamCancellationTests
{
    private class TestDbConnection : DbConnection
    {
        private readonly DataTable _table;
        private readonly Action _onDispose;

        public TestDbConnection(DataTable table, Action onDispose)
        {
            _table = table;
            _onDispose = onDispose;
        }

        public override string ConnectionString { get; set; } = string.Empty;
        public override string Database => string.Empty;
        public override string DataSource => string.Empty;
        public override string ServerVersion => string.Empty;
        public override ConnectionState State => ConnectionState.Open;

        public override void ChangeDatabase(string databaseName) { }
        public override void Close() { }
        public override void Open() { }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
        protected override DbCommand CreateDbCommand() => new TestDbCommand(_table, _onDispose);
    }

    private class TestDbCommand : DbCommand
    {
        private readonly DataTable _table;
        private readonly Action _onDispose;

        public TestDbCommand(DataTable table, Action onDispose)
        {
            _table = table;
            _onDispose = onDispose;
        }

        public override string? CommandText { get; set; }
        public override int CommandTimeout { get; set; }
        public override CommandType CommandType { get; set; }
        public override bool DesignTimeVisible { get; set; }
        public override UpdateRowSource UpdatedRowSource { get; set; }
        protected override DbConnection? DbConnection { get; set; }
        protected override DbParameterCollection DbParameterCollection { get; } = new TestDbParameterCollection();
        protected override DbTransaction? DbTransaction { get; set; }

        public override void Cancel() { }
        public override int ExecuteNonQuery() => throw new NotSupportedException();
        public override object? ExecuteScalar() => throw new NotSupportedException();
        public override void Prepare() { }
        protected override DbParameter CreateDbParameter() => new TestDbParameter();
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => new TestDbDataReader(_table, _onDispose);
        protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken) =>
            Task.FromResult<DbDataReader>(new TestDbDataReader(_table, _onDispose));
    }

    private class TestDbDataReader : DbDataReader
    {
        private readonly DataTableReader _reader;
        private readonly Action _onDispose;

        public TestDbDataReader(DataTable table, Action onDispose)
        {
            _reader = table.CreateDataReader();
            _onDispose = onDispose;
        }

        public override int FieldCount => _reader.FieldCount;
        public override bool HasRows => _reader.HasRows;
        public override bool IsClosed => _reader.IsClosed;
        public override int Depth => _reader.Depth;
        public override int RecordsAffected => 0;
        public override object this[int ordinal] => _reader[ordinal];
        public override object this[string name] => _reader[name];

        public override string GetName(int ordinal) => _reader.GetName(ordinal);
        public override string GetDataTypeName(int ordinal) => _reader.GetDataTypeName(ordinal);
        public override Type GetFieldType(int ordinal) => _reader.GetFieldType(ordinal);
        public override object GetValue(int ordinal) => _reader.GetValue(ordinal);
        public override int GetValues(object[] values) => _reader.GetValues(values);
        public override int GetOrdinal(string name) => _reader.GetOrdinal(name);
        public override bool GetBoolean(int ordinal) => _reader.GetBoolean(ordinal);
        public override byte GetByte(int ordinal) => _reader.GetByte(ordinal);
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => _reader.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
        public override char GetChar(int ordinal) => _reader.GetChar(ordinal);
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => _reader.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
        public override Guid GetGuid(int ordinal) => _reader.GetGuid(ordinal);
        public override short GetInt16(int ordinal) => _reader.GetInt16(ordinal);
        public override int GetInt32(int ordinal) => _reader.GetInt32(ordinal);
        public override long GetInt64(int ordinal) => _reader.GetInt64(ordinal);
        public override float GetFloat(int ordinal) => _reader.GetFloat(ordinal);
        public override double GetDouble(int ordinal) => _reader.GetDouble(ordinal);
        public override string GetString(int ordinal) => _reader.GetString(ordinal);
        public override decimal GetDecimal(int ordinal) => _reader.GetDecimal(ordinal);
        public override DateTime GetDateTime(int ordinal) => _reader.GetDateTime(ordinal);
        public override bool IsDBNull(int ordinal) => _reader.IsDBNull(ordinal);
        public override IEnumerator GetEnumerator() => ((IEnumerable)_reader).GetEnumerator();

        public override bool Read() => _reader.Read();
        public override Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(_reader.Read());
        }

        public override bool NextResult() => _reader.NextResult();
        public override void Close() => _reader.Close();
        public override DataTable GetSchemaTable() => _reader.GetSchemaTable();

        public override ValueTask DisposeAsync()
        {
            _onDispose();
            _reader.Dispose();
            return ValueTask.CompletedTask;
        }
    }

    private class TestDbParameterCollection : DbParameterCollection
    {
        public override int Add(object value) => 0;
        public override void AddRange(Array values) { }
        public override void Clear() { }
        public override bool Contains(string value) => false;
        public override bool Contains(object value) => false;
        public override void CopyTo(Array array, int index) { }
        public override int Count => 0;
        public override bool IsFixedSize => false;
        public override bool IsReadOnly => false;
        public override bool IsSynchronized => false;
        public override object SyncRoot => new object();
        public override int IndexOf(string parameterName) => -1;
        public override int IndexOf(object value) => -1;
        public override void Insert(int index, object value) { }
        public override void Remove(object value) { }
        public override void RemoveAt(string parameterName) { }
        public override void RemoveAt(int index) { }
        protected override DbParameter GetParameter(string parameterName) => throw new NotSupportedException();
        protected override DbParameter GetParameter(int index) => throw new NotSupportedException();
        protected override void SetParameter(string parameterName, DbParameter value) { }
        protected override void SetParameter(int index, DbParameter value) { }
        public override System.Collections.IEnumerator GetEnumerator() => Array.Empty<object>().GetEnumerator();
    }

    private class TestDbParameter : DbParameter
    {
        public override DbType DbType { get; set; }
        public override ParameterDirection Direction { get; set; }
        public override bool IsNullable { get; set; }
        public override string ParameterName { get; set; } = string.Empty;
        public override string SourceColumn { get; set; } = string.Empty;
        public override object? Value { get; set; }
        public override bool SourceColumnNullMapping { get; set; }
        public override int Size { get; set; }
        public override byte Precision { get; set; }
        public override byte Scale { get; set; }
        public override void ResetDbType() { }
    }

    private class TestClient : DatabaseClientBase
    {
        public IAsyncEnumerable<DataRow> Stream(DbConnection connection, CancellationToken token) => ExecuteQueryStreamAsync(connection, null, "q", null, token);
    }

    [Fact]
    public async Task ExecuteQueryStreamAsync_DisposesReader_OnCancellation()
    {
        var table = new DataTable();
        table.Columns.Add("id", typeof(int));
        var r1 = table.NewRow();
        r1["id"] = 1;
        table.Rows.Add(r1);
        var r2 = table.NewRow();
        r2["id"] = 2;
        table.Rows.Add(r2);

        var disposed = false;
        var connection = new TestDbConnection(table, () => disposed = true);
        var client = new TestClient();

        using var cts = new CancellationTokenSource();
        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await foreach (var row in client.Stream(connection, cts.Token).WithCancellation(cts.Token))
            {
                cts.Cancel();
            }
        });

        Assert.True(disposed);
    }
}
