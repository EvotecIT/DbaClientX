using System;
#pragma warning disable CS8765 // Nullability of parameter doesn't match overridden member
#pragma warning disable CS8764 // Nullability of return type doesn't match overridden member
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace DbaClientX.Tests;

public class RetryTests
{
    private class TransientTestException : Exception { }
    private class ProviderCancellationTestException : Exception { }

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
            public override int ExecuteNonQuery()
            {
                if (_connection.ShouldThrow())
                {
                    throw new TransientTestException();
                }
                return 1;
            }
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
            public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken) => Task.FromResult(ExecuteNonQuery());
            public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken) => Task.FromResult(ExecuteScalar());
        }

        internal class FakeParameterCollection : DbParameterCollection
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

        internal class FakeParameter : DbParameter
        {
            public override DbType DbType { get; set; }
            public override ParameterDirection Direction { get; set; }
            public override bool IsNullable { get; set; }
            public override string ParameterName { get; set; } = string.Empty;
            public override string SourceColumn { get; set; } = string.Empty;
            public override object Value { get; set; } = new object();
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
        public Task<T> RunOperationAsync<T>(Func<Task<T>> operation, CancellationToken token = default) => ExecuteWithRetryAsync(operation, token);
    }

    private sealed class CancellationNormalizationClient : DBAClientX.DatabaseClientBase
    {
        protected override bool IsProviderCancellationException(Exception exception)
            => exception is ProviderCancellationTestException || base.IsProviderCancellationException(exception);

        public Exception CreatePublicExecutionException(Exception exception, CancellationToken token)
            => CreateQueryExecutionOrCancellationException("failed", "q", exception, token);

        public bool IsCallerCancellationException(Exception exception, CancellationToken token)
            => IsCallerCancellation(exception, token);

        public Task<DataSet> ReadStoredProcedureAsync(DbCommand command, CancellationToken token)
            => ReadStoredProcedureResultsAsync(command, token);
    }

    private sealed class TokenlessCancellationQueryClient : DBAClientX.DatabaseClientBase
    {
        public Task<object?> RunQueryAsync(DbConnection connection, CancellationToken token)
            => ExecuteQueryAsync(connection, null, "q", cancellationToken: token);
    }

    private sealed class TokenlessCancellationConnection : DbConnection
    {
        private readonly CancellationTokenSource _cancellationSource;

        public TokenlessCancellationConnection(CancellationTokenSource cancellationSource)
            => _cancellationSource = cancellationSource;

        public override string ConnectionString { get; set; } = string.Empty;
        public override string Database => string.Empty;
        public override string DataSource => string.Empty;
        public override string ServerVersion => string.Empty;
        public override ConnectionState State => ConnectionState.Open;
        public override void ChangeDatabase(string databaseName) { }
        public override void Close() { }
        public override void Open() { }
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
        protected override DbCommand CreateDbCommand() => new TokenlessCancellationCommand(this, _cancellationSource);
    }

    private sealed class TokenlessCancellationCommand : DbCommand
    {
        private readonly DbConnection _connection;
        private readonly CancellationTokenSource _cancellationSource;

        public TokenlessCancellationCommand(DbConnection connection, CancellationTokenSource cancellationSource)
        {
            _connection = connection;
            _cancellationSource = cancellationSource;
        }

        public override string CommandText { get; set; } = string.Empty;
        public override int CommandTimeout { get; set; }
        public override CommandType CommandType { get; set; }
        public override bool DesignTimeVisible { get; set; }
        public override UpdateRowSource UpdatedRowSource { get; set; }
        protected override DbConnection DbConnection { get => _connection; set { } }
        protected override DbParameterCollection DbParameterCollection { get; } = new TransientConnection.FakeParameterCollection();
        protected override DbTransaction DbTransaction { get; set; } = null!;
        public override void Cancel() { }
        public override int ExecuteNonQuery() => throw new NotSupportedException();
        public override object? ExecuteScalar() => throw new NotSupportedException();
        public override void Prepare() { }
        protected override DbParameter CreateDbParameter() => new TransientConnection.FakeParameter();
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => throw new NotSupportedException();
        protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            _cancellationSource.Cancel();
            return Task.FromException<DbDataReader>(new OperationCanceledException());
        }
    }

    private class RetryNonQueryClient : DBAClientX.DatabaseClientBase
    {
        protected override bool IsTransient(Exception ex) => ex is TransientTestException;
        public int Run(DbConnection connection) => ExecuteNonQuery(connection, null, "q");
        public Task<int> RunAsync(DbConnection connection, CancellationToken token = default) => ExecuteNonQueryAsync(connection, null, "q", cancellationToken: token);
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
    public void ExecuteNonQuery_RetriesTransientErrors()
    {
        using var client = new RetryNonQueryClient { MaxRetryAttempts = 3, RetryDelay = TimeSpan.Zero, RetryNonQueryOperations = true };
        var connection = new TransientConnection(2);
        var result = client.Run(connection);
        Assert.Equal(1, result);
        Assert.Equal(3, connection.Attempts);
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_RetriesTransientErrors()
    {
        using var client = new RetryNonQueryClient { MaxRetryAttempts = 3, RetryDelay = TimeSpan.Zero, RetryNonQueryOperations = true };
        var connection = new TransientConnection(1);
        var result = await client.RunAsync(connection);
        Assert.Equal(1, result);
        Assert.Equal(2, connection.Attempts);
    }

    [Fact]
    public void ExecuteNonQuery_DoesNotRetryByDefault()
    {
        using var client = new RetryNonQueryClient { MaxRetryAttempts = 3, RetryDelay = TimeSpan.Zero };
        var connection = new TransientConnection(2);

        Assert.Throws<TransientTestException>(() => client.Run(connection));
        Assert.Equal(1, connection.Attempts);
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_DoesNotRetryByDefault()
    {
        using var client = new RetryNonQueryClient { MaxRetryAttempts = 3, RetryDelay = TimeSpan.Zero };
        var connection = new TransientConnection(2);

        await Assert.ThrowsAsync<TransientTestException>(() => client.RunAsync(connection));
        Assert.Equal(1, connection.Attempts);
    }

    [Fact]
    public void MaxRetryAttempts_Negative_Throws()
    {
        var client = new RetryClient();
        Assert.Throws<ArgumentOutOfRangeException>(() => client.MaxRetryAttempts = -1);
    }

    [Fact]
    public void RetryDelay_Negative_Throws()
    {
        var client = new RetryClient();
        Assert.Throws<ArgumentOutOfRangeException>(() => client.RetryDelay = TimeSpan.FromMilliseconds(-1));
    }

    [Fact]
    public async Task ExecuteWithRetryAsync_WithPreCancelledToken_DoesNotInvokeOperation()
    {
        using var client = new RetryClient { MaxRetryAttempts = 3, RetryDelay = TimeSpan.Zero };
        using var cts = new CancellationTokenSource();
        var attempts = 0;
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            client.RunOperationAsync(() =>
            {
                attempts++;
                return Task.FromResult(1);
            }, cts.Token));

        Assert.Equal(0, attempts);
    }

    [Fact]
    public async Task ExecuteWithRetryAsync_WhenCancelledAfterTransientFailure_ThrowsOperationCanceled()
    {
        using var client = new RetryClient { MaxRetryAttempts = 3, RetryDelay = TimeSpan.Zero };
        using var cts = new CancellationTokenSource();
        var attempts = 0;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            client.RunOperationAsync<int>(() =>
            {
                attempts++;
                if (attempts == 1)
                {
                    cts.Cancel();
                    throw new TransientTestException();
                }

                return Task.FromResult(1);
            }, cts.Token));

        Assert.Equal(1, attempts);
    }

    [Fact]
    public void CreatePublicExecutionException_WhenProviderReportsCancelledCommand_NormalizesCallerCancellation()
    {
        using var client = new CancellationNormalizationClient();
        using var cancellation = new CancellationTokenSource();
        var providerException = new ProviderCancellationTestException();
        cancellation.Cancel();

        var exception = Assert.IsType<OperationCanceledException>(
            client.CreatePublicExecutionException(providerException, cancellation.Token));

        Assert.Equal(cancellation.Token, exception.CancellationToken);
        Assert.Same(providerException, exception.InnerException);
    }

    [Fact]
    public void CreatePublicExecutionException_WhenUnrelatedFailureRacesCancellation_PreservesQueryFailureContract()
    {
        using var client = new CancellationNormalizationClient();
        using var cancellation = new CancellationTokenSource();
        var providerException = new InvalidOperationException("database failure");
        cancellation.Cancel();

        var exception = Assert.IsType<DBAClientX.DbaQueryExecutionException>(
            client.CreatePublicExecutionException(providerException, cancellation.Token));

        Assert.Same(providerException, exception.InnerException);
    }

    [Fact]
    public void IsCallerCancellation_WhenFailureOnlyWrapsCallerCancellation_ReturnsFalse()
    {
        using var client = new CancellationNormalizationClient();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var exception = new InvalidOperationException(
            "mapper failed",
            new OperationCanceledException(cancellation.Token));

        Assert.False(client.IsCallerCancellationException(exception, cancellation.Token));
    }

    [Fact]
    public async Task ReadStoredProcedureResultsAsync_WhenProviderThrowsTokenlessCancellation_NormalizesCallerCancellation()
    {
        using var cancellation = new CancellationTokenSource();
        using var client = new CancellationNormalizationClient();
        using var connection = new TokenlessCancellationConnection(cancellation);
        using var command = new TokenlessCancellationCommand(connection, cancellation);

        var exception = await Assert.ThrowsAsync<OperationCanceledException>(() =>
            client.ReadStoredProcedureAsync(command, cancellation.Token));

        Assert.Equal(cancellation.Token, exception.CancellationToken);
        var providerException = Assert.IsType<OperationCanceledException>(exception.InnerException);
        Assert.Equal(default, providerException.CancellationToken);
    }

    [Fact]
    public async Task ExecuteQueryAsync_WhenProviderThrowsTokenlessCancellation_NormalizesCallerCancellation()
    {
        using var cancellation = new CancellationTokenSource();
        using var client = new TokenlessCancellationQueryClient();
        using var connection = new TokenlessCancellationConnection(cancellation);

        var exception = await Assert.ThrowsAsync<OperationCanceledException>(() =>
            client.RunQueryAsync(connection, cancellation.Token));

        Assert.Equal(cancellation.Token, exception.CancellationToken);
        var providerException = Assert.IsType<OperationCanceledException>(exception.InnerException);
        Assert.Equal(default, providerException.CancellationToken);
    }
}
