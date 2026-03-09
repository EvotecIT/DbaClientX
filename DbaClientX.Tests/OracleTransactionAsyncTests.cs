using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;
using Xunit;

namespace DbaClientX.Tests;

public class OracleTransactionAsyncTests
{
    private class FakeOracleConnection
    {
        public bool BeginCalled { get; private set; }
        public IsolationLevel? Level { get; private set; }

        public Task<FakeOracleTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
            => BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);

        public async Task<FakeOracleTransaction> BeginTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken = default)
        {
            BeginCalled = true;
            Level = isolationLevel;
            await Task.Yield();
            return new FakeOracleTransaction(this);
        }
    }

    private class FakeOracleTransaction
    {
        private readonly FakeOracleConnection _connection;
        public bool CommitCalled { get; private set; }
        public bool RollbackCalled { get; private set; }

        public FakeOracleTransaction(FakeOracleConnection connection)
        {
            _connection = connection;
        }

        public Task CommitAsync(CancellationToken cancellationToken = default)
        {
            CommitCalled = true;
            return Task.CompletedTask;
        }

        public Task RollbackAsync(CancellationToken cancellationToken = default)
        {
            RollbackCalled = true;
            return Task.CompletedTask;
        }
    }

    private class TestOracle : DBAClientX.Oracle
    {
        private readonly object _syncRoot = new();
        public FakeOracleConnection? Connection { get; private set; }
        public FakeOracleTransaction? Transaction { get; private set; }

        public override async Task BeginTransactionAsync(string host, string serviceName, string username, string password, CancellationToken cancellationToken = default)
        {
            lock (_syncRoot)
            {
                if (Transaction != null)
                {
                    throw new DBAClientX.DbaTransactionException("Transaction already started.");
                }
            }

            var connection = new FakeOracleConnection();
            var transaction = await connection.BeginTransactionAsync(cancellationToken);

            lock (_syncRoot)
            {
                if (Transaction != null)
                {
                    throw new DBAClientX.DbaTransactionException("Transaction already started.");
                }

                Connection = connection;
                Transaction = transaction;
            }
        }

        public override async Task BeginTransactionAsync(string host, string serviceName, string username, string password, IsolationLevel isolationLevel, CancellationToken cancellationToken = default)
        {
            lock (_syncRoot)
            {
                if (Transaction != null)
                {
                    throw new DBAClientX.DbaTransactionException("Transaction already started.");
                }
            }

            var connection = new FakeOracleConnection();
            var transaction = await connection.BeginTransactionAsync(isolationLevel, cancellationToken);

            lock (_syncRoot)
            {
                if (Transaction != null)
                {
                    throw new DBAClientX.DbaTransactionException("Transaction already started.");
                }

                Connection = connection;
                Transaction = transaction;
            }
        }

        public override async Task CommitAsync(CancellationToken cancellationToken = default)
        {
            if (Transaction == null)
            {
                throw new DBAClientX.DbaTransactionException("No active transaction.");
            }

            await Transaction.CommitAsync(cancellationToken);
            Transaction = null;
        }

        public override async Task RollbackAsync(CancellationToken cancellationToken = default)
        {
            if (Transaction == null)
            {
                throw new DBAClientX.DbaTransactionException("No active transaction.");
            }

            await Transaction.RollbackAsync(cancellationToken);
            Transaction = null;
        }

        public override Task<object?> QueryAsync(string host, string serviceName, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, OracleDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            if (useTransaction && Transaction == null)
            {
                throw new DBAClientX.DbaTransactionException("Transaction has not been started.");
            }

            return Task.FromResult<object?>(null);
        }
    }

    [Fact]
    public async Task BeginTransactionAsync_UsesConnection()
    {
        using var oracle = new TestOracle();
        await oracle.BeginTransactionAsync("h", "svc", "u", "p");
        Assert.NotNull(oracle.Connection);
        Assert.True(oracle.Connection!.BeginCalled);
        Assert.NotNull(oracle.Transaction);
    }

    [Fact]
    public async Task BeginTransactionAsync_WhenCalledConcurrently_Throws()
    {
        using var oracle = new TestOracle();

        var tasks = new[]
        {
            Task.Run(() => oracle.BeginTransactionAsync("h", "svc", "u", "p")),
            Task.Run(() => oracle.BeginTransactionAsync("h", "svc", "u", "p"))
        };

        await Assert.ThrowsAsync<DBAClientX.DbaTransactionException>(() => Task.WhenAll(tasks));
        Assert.NotNull(oracle.Transaction);
    }

    [Fact]
    public async Task CommitAsync_CallsCommitOnTransaction()
    {
        using var oracle = new TestOracle();
        await oracle.BeginTransactionAsync("h", "svc", "u", "p");
        var transaction = oracle.Transaction!;
        await oracle.CommitAsync();
        Assert.True(transaction.CommitCalled);
        Assert.Null(oracle.Transaction);
    }

    [Fact]
    public async Task RollbackAsync_CallsRollbackOnTransaction()
    {
        using var oracle = new TestOracle();
        await oracle.BeginTransactionAsync("h", "svc", "u", "p");
        var transaction = oracle.Transaction!;
        await oracle.RollbackAsync();
        Assert.True(transaction.RollbackCalled);
        Assert.Null(oracle.Transaction);
    }

    [Fact]
    public async Task BeginTransactionAsync_WithIsolationLevel_PassesIsolationLevel()
    {
        using var oracle = new TestOracle();
        await oracle.BeginTransactionAsync("h", "svc", "u", "p", IsolationLevel.Serializable);
        Assert.NotNull(oracle.Connection);
        Assert.Equal(IsolationLevel.Serializable, oracle.Connection!.Level);
    }

    private class OpenFailureTransactionOracle : DBAClientX.Oracle
    {
        public int DisposeCalls { get; private set; }

        protected override Task OpenConnectionAsync(OracleConnection connection, CancellationToken cancellationToken)
            => Task.FromException(new InvalidOperationException("boom"));

        protected override void DisposeConnection(OracleConnection connection)
            => DisposeCalls++;
    }

    [Fact]
    public async Task BeginTransactionAsync_WhenOpenFails_DisposesConnectionAndResetsState()
    {
        using var oracle = new OpenFailureTransactionOracle();

        await Assert.ThrowsAsync<InvalidOperationException>(() => oracle.BeginTransactionAsync("h", "svc", "u", "p"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => oracle.BeginTransactionAsync("h", "svc", "u", "p"));

        Assert.False(oracle.IsInTransaction);
        Assert.Equal(2, oracle.DisposeCalls);
    }
}
