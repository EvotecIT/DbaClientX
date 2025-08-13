using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using DBAClientX;
using Xunit;
using System.Data.SqlClient;

namespace DbaClientX.Tests;

public class DatabaseClientBasePerformanceTests
{
    private class FakeDbConnection : DbConnection
    {
        private readonly DbDataReader _reader;
        public FakeDbConnection(DbDataReader reader) => _reader = reader;
        public override string ConnectionString { get; set; } = string.Empty;
        public override string Database => string.Empty;
        public override string DataSource => string.Empty;
        public override string ServerVersion => string.Empty;
        public override ConnectionState State => ConnectionState.Open;
        public override void ChangeDatabase(string databaseName) { }
        public override void Close() { }
        public override void Open() { }
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotImplementedException();
        protected override DbCommand CreateDbCommand() => new FakeDbCommand(_reader);
    }

    private class FakeDbCommand : DbCommand
    {
        private readonly DbDataReader _reader;
        public FakeDbCommand(DbDataReader reader) => _reader = reader;
        public override string CommandText { get; set; } = string.Empty;
        public override int CommandTimeout { get; set; }
        public override CommandType CommandType { get; set; } = CommandType.Text;
        protected override DbConnection DbConnection { get; set; } = null!;
        protected override DbParameterCollection DbParameterCollection { get; } = new SqlCommand().Parameters;
        protected override DbTransaction? DbTransaction { get; set; }
        public override bool DesignTimeVisible { get; set; }
        public override UpdateRowSource UpdatedRowSource { get; set; }
        public override void Cancel() { }
        public override int ExecuteNonQuery() => throw new NotSupportedException();
        public override object? ExecuteScalar() => throw new NotSupportedException();
        public override void Prepare() { }
        protected override DbParameter CreateDbParameter() => new FakeDbParameter();
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => _reader;
    }

    private class FakeDbParameter : DbParameter
    {
        public override DbType DbType { get; set; }
        public override ParameterDirection Direction { get; set; }
        public override bool IsNullable { get; set; }
        public override string ParameterName { get; set; } = string.Empty;
        public override string SourceColumn { get; set; } = string.Empty;
        public override object? Value { get; set; }
        public override bool SourceColumnNullMapping { get; set; }
        public override int Size { get; set; }
        public override void ResetDbType() { }
    }


    private class FakeDataReader : DbDataReader
    {
        private readonly int[][] _rows;
        private int _index = -1;
        public bool ThrowOnSecondRead { get; set; }
        public bool ThrowOnNextResult { get; set; }
        public FakeDataReader(params int[][] rows) => _rows = rows;
        public override bool Read()
        {
            if (_index == 0 && ThrowOnSecondRead)
            {
                throw new InvalidOperationException("Read called more than once");
            }
            _index++;
            return _index < _rows.Length;
        }
        public override bool NextResult()
        {
            if (ThrowOnNextResult)
            {
                throw new InvalidOperationException("NextResult called");
            }
            return false;
        }
        public override int FieldCount => 1;
        public override string GetName(int ordinal) => "id";
        public override Type GetFieldType(int ordinal) => typeof(int);
        public override object GetValue(int ordinal) => _rows[_index][ordinal];
        public override int GetValues(object[] values)
        {
            values[0] = GetValue(0);
            return 1;
        }
        public override int GetOrdinal(string name) => 0;
        public override DataTable GetSchemaTable()
        {
            var schema = new DataTable();
            schema.Columns.Add("ColumnName", typeof(string));
            schema.Columns.Add("ColumnOrdinal", typeof(int));
            schema.Columns.Add("DataType", typeof(Type));
            var row = schema.NewRow();
            row["ColumnName"] = "id";
            row["ColumnOrdinal"] = 0;
            row["DataType"] = typeof(int);
            schema.Rows.Add(row);
            return schema;
        }
        public override bool IsDBNull(int ordinal) => false;
        public override int Depth => 0;
        public override bool HasRows => _rows.Length > 0;
        public override bool IsClosed => false;
        public override int RecordsAffected => 0;
        public override object this[int ordinal] => throw new NotImplementedException();
        public override object this[string name] => throw new NotImplementedException();
        public override bool GetBoolean(int ordinal) => throw new NotImplementedException();
        public override byte GetByte(int ordinal) => throw new NotImplementedException();
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => throw new NotImplementedException();
        public override char GetChar(int ordinal) => throw new NotImplementedException();
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => throw new NotImplementedException();
        public override string GetDataTypeName(int ordinal) => typeof(int).Name;
        public override DateTime GetDateTime(int ordinal) => throw new NotImplementedException();
        public override decimal GetDecimal(int ordinal) => throw new NotImplementedException();
        public override double GetDouble(int ordinal) => throw new NotImplementedException();
        public override IEnumerator GetEnumerator() => throw new NotImplementedException();
        public override float GetFloat(int ordinal) => throw new NotImplementedException();
        public override Guid GetGuid(int ordinal) => throw new NotImplementedException();
        public override short GetInt16(int ordinal) => (short)GetValue(ordinal);
        public override int GetInt32(int ordinal) => (int)GetValue(ordinal);
        public override long GetInt64(int ordinal) => (int)GetValue(ordinal);
        public override string GetString(int ordinal) => GetValue(ordinal).ToString()!;
    }

    private class TestClient : DatabaseClientBase
    {
        public object? Execute(DbConnection connection) => ExecuteQuery(connection, null, string.Empty, null);
    }

    [Fact]
    public void ExecuteQuery_DataRow_StopsAfterFirstRow()
    {
        var reader = new FakeDataReader(new[] { 1 }, new[] { 2 }) { ThrowOnSecondRead = true };
        using var connection = new FakeDbConnection(reader);
        using var client = new TestClient { ReturnType = ReturnType.DataRow };
        var result = client.Execute(connection);
        var row = Assert.IsType<DataRow>(result);
        Assert.Equal(1, row[0]);
    }

    [Fact]
    public void ExecuteQuery_DataTable_ReturnsTable()
    {
        var reader = new FakeDataReader(new[] { 1 });
        using var connection = new FakeDbConnection(reader);
        using var client = new TestClient { ReturnType = ReturnType.DataTable };
        var result = client.Execute(connection);
        var table = Assert.IsType<DataTable>(result);
        Assert.Equal(1, table.Rows[0][0]);
    }
}
