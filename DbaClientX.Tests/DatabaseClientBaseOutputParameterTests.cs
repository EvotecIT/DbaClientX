using System;
#pragma warning disable CS8765
#pragma warning disable CS8764
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using DBAClientX;

namespace DbaClientX.Tests;

public class DatabaseClientBaseOutputParameterTests
{
    private sealed class TestClient : DatabaseClientBase
    {
        public object? RunQuery(DbConnection connection, IDictionary<string, object?> parameters, IDictionary<string, ParameterDirection> directions)
            => ExecuteQuery(connection, null, "q", parameters, parameterDirections: directions);

        public Task<object?> RunQueryAsync(DbConnection connection, IDictionary<string, object?> parameters, IDictionary<string, ParameterDirection> directions)
            => ExecuteQueryAsync(connection, null, "q", parameters, parameterDirections: directions);

        public Task<IReadOnlyList<T>> RunMappedQueryAsync<T>(
            DbConnection connection,
            Func<IDataRecord, T> map,
            Action<IDataRecord>? initialize = null,
            IDictionary<string, object?>? parameters = null,
            IDictionary<string, ParameterDirection>? directions = null)
            => ExecuteMappedQueryAsync(connection, null, "q", map, initialize, parameters, parameterDirections: directions);

        public IAsyncEnumerable<T> RunMappedStream<T>(
            DbConnection connection,
            Func<IDataRecord, T> map,
            Action<IDataRecord>? initialize = null,
            IDictionary<string, object?>? parameters = null,
            IDictionary<string, ParameterDirection>? directions = null)
            => ExecuteMappedQueryStreamAsync(connection, null, "q", map, initialize, parameters, parameterDirections: directions);
    }

    private sealed class FakeConnection : DbConnection
    {
        public override string ConnectionString { get; set; } = string.Empty;
        public override string Database => string.Empty;
        public override string DataSource => string.Empty;
        public override string ServerVersion => string.Empty;
        public override ConnectionState State => ConnectionState.Open;
        public FakeCommand? LastCommand { get; private set; }
        public override void ChangeDatabase(string databaseName) { }
        public override void Close() { }
        public override void Open() { }
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
        protected override DbCommand CreateDbCommand()
        {
            LastCommand = new FakeCommand();
            return LastCommand;
        }
    }

    private sealed class FakeCommand : DbCommand
    {
        private readonly FakeParameterCollection _parameters = new();

        public override string CommandText { get; set; } = string.Empty;
        public override int CommandTimeout { get; set; }
        public override CommandType CommandType { get; set; }
        public CommandBehavior? LastReaderBehavior { get; private set; }
        public override bool DesignTimeVisible { get; set; }
        public override UpdateRowSource UpdatedRowSource { get; set; }
        protected override DbConnection DbConnection { get; set; } = null!;
        protected override DbParameterCollection DbParameterCollection => _parameters;
        protected override DbTransaction? DbTransaction { get; set; }
        public override void Cancel() { }
        public override int ExecuteNonQuery() => throw new NotSupportedException();
        public override object? ExecuteScalar() => throw new NotSupportedException();
        public override void Prepare() { }
        protected override DbParameter CreateDbParameter() => new FakeParameter();

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            LastReaderBehavior = behavior;
            SetOutputParameterValues();
            return new FakeDataReader();
        }

        protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            LastReaderBehavior = behavior;
            SetOutputParameterValues();
            return Task.FromResult<DbDataReader>(new FakeDataReader());
        }

        private void SetOutputParameterValues()
        {
            foreach (FakeParameter parameter in _parameters)
            {
                if (parameter.Direction != ParameterDirection.Input)
                {
                    parameter.Value = 5;
                }
            }
        }
    }

    private sealed class FakeParameterCollection : DbParameterCollection
    {
        private readonly List<DbParameter> _items = new();

        public override int Count => _items.Count;
        public override object SyncRoot { get; } = new();
        public override int Add(object value)
        {
            _items.Add((DbParameter)value);
            return _items.Count - 1;
        }
        public override void AddRange(Array values)
        {
            foreach (var value in values)
            {
                Add(value!);
            }
        }
        public override void Clear() => _items.Clear();
        public override bool Contains(object value) => _items.Contains((DbParameter)value);
        public override bool Contains(string value) => _items.Exists(p => string.Equals(p.ParameterName, value, StringComparison.OrdinalIgnoreCase));
        public override void CopyTo(Array array, int index) => _items.ToArray().CopyTo(array, index);
        public override IEnumerator GetEnumerator() => _items.GetEnumerator();
        public override int IndexOf(object value) => _items.IndexOf((DbParameter)value);
        public override int IndexOf(string parameterName) => _items.FindIndex(p => string.Equals(p.ParameterName, parameterName, StringComparison.OrdinalIgnoreCase));
        public override void Insert(int index, object value) => _items.Insert(index, (DbParameter)value);
        public override void Remove(object value) => _items.Remove((DbParameter)value);
        public override void RemoveAt(int index) => _items.RemoveAt(index);
        public override void RemoveAt(string parameterName)
        {
            var index = IndexOf(parameterName);
            if (index >= 0)
            {
                _items.RemoveAt(index);
            }
        }
        protected override DbParameter GetParameter(int index) => _items[index];
        protected override DbParameter GetParameter(string parameterName) => _items[IndexOf(parameterName)];
        protected override void SetParameter(int index, DbParameter value) => _items[index] = value;
        protected override void SetParameter(string parameterName, DbParameter value)
        {
            var index = IndexOf(parameterName);
            if (index >= 0)
            {
                _items[index] = value;
            }
            else
            {
                _items.Add(value);
            }
        }
    }

    private sealed class FakeParameter : DbParameter
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

    private sealed class FakeDataReader : DbDataReader
    {
        private int _index = -1;

        public override int FieldCount => 1;
        public override bool HasRows => true;
        public override bool IsClosed => false;
        public override int RecordsAffected => 0;
        public override int Depth => 0;
        public override object this[int ordinal] => GetValue(ordinal);
        public override object this[string name] => GetValue(0);

        public override bool Read()
        {
            _index++;
            return _index == 0;
        }

        public override Task<bool> ReadAsync(CancellationToken cancellationToken)
            => Task.FromResult(Read());

        public override bool NextResult() => false;

        public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
            => Task.FromResult(false);

        public override string GetName(int ordinal) => "id";
        public override string GetDataTypeName(int ordinal) => typeof(int).Name;
        public override Type GetFieldType(int ordinal) => typeof(int);
        public override object GetValue(int ordinal) => 1;
        public override int GetValues(object[] values)
        {
            values[0] = 1;
            return 1;
        }
        public override int GetOrdinal(string name) => 0;
        public override bool IsDBNull(int ordinal) => false;
        public override DataTable GetSchemaTable()
        {
            var table = new DataTable();
            table.Columns.Add("ColumnName", typeof(string));
            table.Columns.Add("ColumnOrdinal", typeof(int));
            table.Columns.Add("DataType", typeof(Type));
            var row = table.NewRow();
            row["ColumnName"] = "id";
            row["ColumnOrdinal"] = 0;
            row["DataType"] = typeof(int);
            table.Rows.Add(row);
            return table;
        }
        public override bool GetBoolean(int ordinal) => throw new NotSupportedException();
        public override byte GetByte(int ordinal) => throw new NotSupportedException();
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
        public override char GetChar(int ordinal) => throw new NotSupportedException();
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
        public override DateTime GetDateTime(int ordinal) => throw new NotSupportedException();
        public override decimal GetDecimal(int ordinal) => throw new NotSupportedException();
        public override double GetDouble(int ordinal) => throw new NotSupportedException();
        public override IEnumerator GetEnumerator() => Array.Empty<object>().GetEnumerator();
        public override float GetFloat(int ordinal) => throw new NotSupportedException();
        public override Guid GetGuid(int ordinal) => throw new NotSupportedException();
        public override short GetInt16(int ordinal) => 1;
        public override int GetInt32(int ordinal) => 1;
        public override long GetInt64(int ordinal) => 1;
        public override string GetString(int ordinal) => "1";
    }

    [Fact]
    public void ExecuteQuery_DataTable_UpdatesOutputParameters()
    {
        using var client = new TestClient { ReturnType = ReturnType.DataTable };
        using var connection = new FakeConnection();
        var parameters = new Dictionary<string, object?> { ["@out"] = null };
        var directions = new Dictionary<string, ParameterDirection> { ["@out"] = ParameterDirection.Output };

        var result = client.RunQuery(connection, parameters, directions);

        Assert.IsType<DataTable>(result);
        Assert.Equal(5, parameters["@out"]);
    }

    [Fact]
    public async Task ExecuteQueryAsync_DataRow_UpdatesOutputParameters()
    {
        using var client = new TestClient { ReturnType = ReturnType.DataRow };
        using var connection = new FakeConnection();
        var parameters = new Dictionary<string, object?> { ["@out"] = null };
        var directions = new Dictionary<string, ParameterDirection> { ["@out"] = ParameterDirection.Output };

        var result = await client.RunQueryAsync(connection, parameters, directions);

        Assert.IsType<DataRow>(result);
        Assert.Equal(5, parameters["@out"]);
    }

    [Fact]
    public void ExecuteQuery_UpdatesExistingOutputParameterKey_CaseInsensitively()
    {
        using var client = new TestClient { ReturnType = ReturnType.DataTable };
        using var connection = new FakeConnection();
        var parameters = new Dictionary<string, object?> { ["@Out"] = null };
        var directions = new Dictionary<string, ParameterDirection> { ["@OUT"] = ParameterDirection.Output };

        client.RunQuery(connection, parameters, directions);

        Assert.Single(parameters);
        Assert.True(parameters.ContainsKey("@Out"));
        Assert.Equal(5, parameters["@Out"]);
    }

    [Fact]
    public async Task ExecuteMappedQueryAsync_MapsRowsAndRunsInitializer()
    {
        using var client = new TestClient();
        using var connection = new FakeConnection();
        var initializeCount = 0;
        var idOrdinal = -1;

        IReadOnlyList<int> rows = await client.RunMappedQueryAsync(
            connection,
            record => record.GetInt32(idOrdinal),
            record => {
                initializeCount++;
                idOrdinal = record.GetOrdinal("id");
            });

        Assert.Equal(new[] { 1 }, rows);
        Assert.Equal(1, initializeCount);
        Assert.Equal(0, idOrdinal);
    }

    [Fact]
    public async Task ExecuteMappedQueryAsync_UsesDefaultReaderBehaviorForMapperCompatibility()
    {
        using var client = new TestClient();
        using var connection = new FakeConnection();

        await client.RunMappedQueryAsync(connection, record => record.GetInt32(0));

        Assert.Equal(CommandBehavior.Default, connection.LastCommand?.LastReaderBehavior);
    }

    [Fact]
    public async Task ExecuteMappedQueryAsync_UpdatesOutputParameters()
    {
        using var client = new TestClient();
        using var connection = new FakeConnection();
        var parameters = new Dictionary<string, object?> { ["@out"] = null };
        var directions = new Dictionary<string, ParameterDirection> { ["@out"] = ParameterDirection.Output };

        IReadOnlyList<int> rows = await client.RunMappedQueryAsync(
            connection,
            record => record.GetInt32(0),
            parameters: parameters,
            directions: directions);

        Assert.Equal(new[] { 1 }, rows);
        Assert.Equal(5, parameters["@out"]);
    }

    [Fact]
    public async Task ExecuteMappedQueryStreamAsync_MapsRowsAndRunsInitializer()
    {
        using var client = new TestClient();
        using var connection = new FakeConnection();
        var rows = new List<int>();
        var initializeCount = 0;
        var idOrdinal = -1;

        await foreach (int row in client.RunMappedStream(
            connection,
            record => record.GetInt32(idOrdinal),
            record => {
                initializeCount++;
                idOrdinal = record.GetOrdinal("id");
            }))
        {
            rows.Add(row);
        }

        Assert.Equal(new[] { 1 }, rows);
        Assert.Equal(1, initializeCount);
        Assert.Equal(0, idOrdinal);
    }

    [Fact]
    public async Task ExecuteMappedQueryStreamAsync_UsesDefaultReaderBehaviorForMapperCompatibility()
    {
        using var client = new TestClient();
        using var connection = new FakeConnection();

        await foreach (int _ in client.RunMappedStream(connection, record => record.GetInt32(0)))
        {
        }

        Assert.Equal(CommandBehavior.Default, connection.LastCommand?.LastReaderBehavior);
    }
}
