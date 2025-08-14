using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace DbaClientX.Tests;

public class RetryTests
{
    private class TransientTestException : Exception { }

    private class TransientConnection : DbConnection
    {
        private readonly int _failuresBeforeSuccess;
        private int _attempts;
        public TransientConnection(int failuresBeforeSuccess)
        {
            _failuresBeforeSuccess = failuresBeforeSuccess;
        }
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
        public override void Close() { }
        public override void ChangeDatabase(string databaseName) { }
        public override void Open() { }
        public override string ConnectionString { get; set; } = string.Empty;
        public override string Database => string.Empty;
        public override ConnectionState State => ConnectionState.Open;
        public override string DataSource => string.Empty;
        public override string ServerVersion => string.Empty;
        protected override DbCommand CreateDbCommand() => new TransientCommand(this);
        internal bool ShouldThrow() => Interlocked.Increment(ref _attempts) <= _failuresBeforeSuccess;
        internal int Attempts => _attempts;

        private class TransientCommand : DbCommand
        {
            private readonly TransientConnection _connection;
            public TransientCommand(TransientConnection connection) => _connection = connection;
            public override string CommandText { get; set; } = string.Empty;
            public override int CommandTimeout { get; set; }
            public override CommandType CommandType { get; set; }
            public override bool DesignTimeVisible { get; set; }
            public override UpdateRowSource UpdatedRowSource { get; set; }
            protected override DbConnection DbConnection { get => _connection; set { } }
            protected override DbParameterCollection DbParameterCollection { get; } = new FakeParameterCollection();
            protected override DbTransaction DbTransaction { get; set; } = null!;
            public override void Cancel() { }
            public override int ExecuteNonQuery() => throw new NotSupportedException();
            public override object? ExecuteScalar()
            {
                if (_connection.ShouldThrow())
                {
                    throw new TransientTestException();
                }
                return 1;
            }
            public override void Prepare() { }
            protected override DbParameter CreateDbParameter() => new FakeParameter();
            protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => throw new NotSupportedException();
            public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken) => Task.FromResult(ExecuteScalar());
        }

        private class FakeParameterCollection : DbParameterCollection
        {
            public override int Count => 0;
            public override object SyncRoot { get; } = new();
            public override int Add(object value) => 0;
            public override void AddRange(Array values) { }
            public override void Clear() { }
            public override bool Contains(object value) => false;
            public override bool Contains(string value) => false;
            public override void CopyTo(Array array, int index) { }
            public override IEnumerator GetEnumerator() => Array.Empty<object>().GetEnumerator();
            public override int IndexOf(object value) => -1;
            public override int IndexOf(string parameterName) => -1;
            public override void Insert(int index, object value) { }
            public override void Remove(object value) { }
            public override void RemoveAt(int index) { }
            public override void RemoveAt(string parameterName) { }
            protected override DbParameter GetParameter(int index) => throw new NotSupportedException();
            protected override DbParameter GetParameter(string parameterName) => throw new NotSupportedException();
            protected override void SetParameter(int index, DbParameter value) { }
            protected override void SetParameter(string parameterName, DbParameter value) { }
        }

        private class FakeParameter : DbParameter
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
    }

    private class RetryClient : DBAClientX.DatabaseClientBase
    {
        protected override bool IsTransient(Exception ex) => ex is TransientTestException;
        public object? Run(DbConnection connection) => ExecuteScalar(connection, null, "q");
        public Task<object?> RunAsync(DbConnection connection, CancellationToken token = default) => ExecuteScalarAsync(connection, null, "q", cancellationToken: token);
    }

    [Fact]
    public void ExecuteScalar_RetriesTransientErrors()
    {
        using var client = new RetryClient { MaxRetryAttempts = 3, RetryDelay = TimeSpan.Zero };
        var connection = new TransientConnection(2);
        var result = client.Run(connection);
        Assert.Equal(1, result);
        Assert.Equal(3, connection.Attempts);
    }

    [Fact]
    public void ExecuteScalar_ThrowsAfterMaxRetries()
    {
        using var client = new RetryClient { MaxRetryAttempts = 2, RetryDelay = TimeSpan.Zero };
        var connection = new TransientConnection(5);
        Assert.Throws<TransientTestException>(() => client.Run(connection));
        Assert.Equal(2, connection.Attempts);
    }

    [Fact]
    public async Task ExecuteScalarAsync_RetriesTransientErrors()
    {
        using var client = new RetryClient { MaxRetryAttempts = 3, RetryDelay = TimeSpan.Zero };
        var connection = new TransientConnection(1);
        var result = await client.RunAsync(connection);
        Assert.Equal(1, result);
        Assert.Equal(2, connection.Attempts);
    }

    [Fact]
    public void MaxRetryAttempts_Negative_Throws()
    {
        var client = new RetryClient();
        Assert.Throws<ArgumentOutOfRangeException>(() => client.MaxRetryAttempts = -1);
    }
}
