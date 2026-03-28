using System.Collections.Generic;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Data;
using MySqlConnector;
using Xunit;

namespace DbaClientX.Tests;

public class MySqlTransactionAsyncTests
{
    private static readonly FieldInfo TransactionConnectionStringField = typeof(DBAClientX.MySql)
        .GetField("_transactionConnectionString", BindingFlags.Instance | BindingFlags.NonPublic)!;
    private static readonly FieldInfo TransactionField = typeof(DBAClientX.MySql)
        .GetField("_transaction", BindingFlags.Instance | BindingFlags.NonPublic)!;
    private static readonly FieldInfo TransactionConnectionField = typeof(DBAClientX.MySql)
        .GetField("_transactionConnection", BindingFlags.Instance | BindingFlags.NonPublic)!;

    private class FakeMySqlConnection
    {
        public bool BeginCalled { get; private set; }
        public IsolationLevel? Level { get; private set; }

        public Task<FakeMySqlTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
            => BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);

        public async Task<FakeMySqlTransaction> BeginTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken = default)
        {
            BeginCalled = true;
            Level = isolationLevel;
            await Task.Yield();
            return new FakeMySqlTransaction(this);
        }
    }

    private class FakeMySqlTransaction
    {
        private readonly FakeMySqlConnection _connection;
        public bool CommitCalled { get; private set; }
        public bool RollbackCalled { get; private set; }

        public FakeMySqlTransaction(FakeMySqlConnection connection)
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

    private class TestMySql : DBAClientX.MySql
    {
        private readonly object _syncRoot = new();
        public FakeMySqlConnection? Connection { get; private set; }
        public FakeMySqlTransaction? Transaction { get; private set; }

        public override async Task BeginTransactionAsync(string host, string database, string username, string password, CancellationToken cancellationToken = default)
        {
            lock (_syncRoot)
            {
                if (Transaction != null)
                {
                    throw new DBAClientX.DbaTransactionException("Transaction already started.");
                }
            }

            var connection = new FakeMySqlConnection();
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

        public override async Task BeginTransactionAsync(string host, string database, string username, string password, IsolationLevel isolationLevel, CancellationToken cancellationToken = default)
        {
            lock (_syncRoot)
            {
                if (Transaction != null)
                {
                    throw new DBAClientX.DbaTransactionException("Transaction already started.");
                }
            }

            var connection = new FakeMySqlConnection();
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

        public override Task<object?> QueryAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, MySqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
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
        using var mySql = new TestMySql();
        await mySql.BeginTransactionAsync("h", "d", "u", "p");
        Assert.NotNull(mySql.Connection);
        Assert.True(mySql.Connection!.BeginCalled);
        Assert.NotNull(mySql.Transaction);
    }

    [Fact]
    public async Task BeginTransactionAsync_WhenCalledConcurrently_Throws()
    {
        using var mySql = new TestMySql();

        var tasks = new[]
        {
            Task.Run(() => mySql.BeginTransactionAsync("h", "d", "u", "p")),
            Task.Run(() => mySql.BeginTransactionAsync("h", "d", "u", "p"))
        };

        await Assert.ThrowsAsync<DBAClientX.DbaTransactionException>(() => Task.WhenAll(tasks));
        Assert.NotNull(mySql.Transaction);
    }

    [Fact]
    public async Task CommitAsync_CallsCommitOnTransaction()
    {
        using var mySql = new TestMySql();
        await mySql.BeginTransactionAsync("h", "d", "u", "p");
        var txn = mySql.Transaction!;
        await mySql.CommitAsync();
        Assert.True(txn.CommitCalled);
        Assert.Null(mySql.Transaction);
    }

    [Fact]
    public async Task RollbackAsync_CallsRollbackOnTransaction()
    {
        using var mySql = new TestMySql();
        await mySql.BeginTransactionAsync("h", "d", "u", "p");
        var txn = mySql.Transaction!;
        await mySql.RollbackAsync();
        Assert.True(txn.RollbackCalled);
        Assert.Null(mySql.Transaction);
    }

    [Fact]
    public async Task BeginTransactionAsync_WithIsolationLevel_PassesIsolationLevel()
    {
        using var mySql = new TestMySql();
        await mySql.BeginTransactionAsync("h", "d", "u", "p", IsolationLevel.Serializable);
        Assert.NotNull(mySql.Connection);
        Assert.Equal(IsolationLevel.Serializable, mySql.Connection!.Level);
    }

    [Fact]
    public async Task RunInTransactionAsync_CommitsAndReturnsResult()
    {
        using var mySql = new TestMySql();

        var result = await mySql.RunInTransactionAsync("h", "d", "u", "p", async (client, token) =>
        {
            await client.QueryAsync("h", "d", "u", "p", "q", useTransaction: true, cancellationToken: token);
            return 42;
        });

        Assert.Equal(42, result);
        Assert.Null(mySql.Transaction);
    }

    [Fact]
    public async Task RunInTransactionAsync_WhenOperationFails_RollsBackAndRethrows()
    {
        using var mySql = new TestMySql();
        FakeMySqlTransaction? txn = null;

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            mySql.RunInTransactionAsync("h", "d", "u", "p", async (_, _) =>
            {
                txn = mySql.Transaction;
                await Task.Yield();
                throw new InvalidOperationException("boom");
            }));

        Assert.Equal("boom", ex.Message);
        Assert.NotNull(txn);
        Assert.True(txn!.RollbackCalled);
        Assert.Null(mySql.Transaction);
    }

    private class OpenFailureTransactionMySql : DBAClientX.MySql
    {
        public int DisposeCalls { get; private set; }

        protected override Task OpenConnectionAsync(MySqlConnection connection, CancellationToken cancellationToken)
            => Task.FromException(new InvalidOperationException("boom"));

        protected override void DisposeConnection(MySqlConnection connection)
            => DisposeCalls++;

        protected override ValueTask DisposeConnectionAsync(MySqlConnection connection)
        {
            DisposeCalls++;
            return default;
        }
    }

    [Fact]
    public async Task BeginTransactionAsync_WhenOpenFails_DisposesConnectionAndResetsState()
    {
        using var mySql = new OpenFailureTransactionMySql();

        await Assert.ThrowsAsync<InvalidOperationException>(() => mySql.BeginTransactionAsync("h", "d", "u", "p"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => mySql.BeginTransactionAsync("h", "d", "u", "p"));

        Assert.False(mySql.IsInTransaction);
        Assert.Equal(2, mySql.DisposeCalls);
    }

    private class BeginFailureTransactionMySql : DBAClientX.MySql
    {
        public int DisposeCalls { get; private set; }

        protected override Task OpenConnectionAsync(MySqlConnection connection, CancellationToken cancellationToken)
            => Task.CompletedTask;

        protected override Task<MySqlTransaction> BeginDbTransactionAsync(MySqlConnection connection, IsolationLevel isolationLevel, CancellationToken cancellationToken)
            => Task.FromException<MySqlTransaction>(new InvalidOperationException("boom"));

        protected override void DisposeConnection(MySqlConnection connection)
            => DisposeCalls++;

        protected override ValueTask DisposeConnectionAsync(MySqlConnection connection)
        {
            DisposeCalls++;
            return default;
        }
    }

    [Fact]
    public async Task BeginTransactionAsync_WhenBeginDbTransactionFails_DisposesConnectionAndResetsState()
    {
        using var mySql = new BeginFailureTransactionMySql();

        await Assert.ThrowsAsync<InvalidOperationException>(() => mySql.BeginTransactionAsync("h", "d", "u", "p"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => mySql.BeginTransactionAsync("h", "d", "u", "p"));

        Assert.False(mySql.IsInTransaction);
        Assert.Equal(2, mySql.DisposeCalls);
    }

    private sealed class AsyncCleanupTransactionMySql : DBAClientX.MySql
    {
        public void SeedTransactionState(string normalizedConnectionString)
        {
            TransactionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(MySqlTransaction)));
            TransactionConnectionStringField.SetValue(this, normalizedConnectionString);
        }

        protected override Task CommitDbTransactionAsync(MySqlTransaction transaction, CancellationToken cancellationToken)
            => Task.CompletedTask;

        protected override Task RollbackDbTransactionAsync(MySqlTransaction transaction, CancellationToken cancellationToken)
            => Task.CompletedTask;

        protected override void DisposeTransactionResources(MySqlTransaction? transaction, MySqlConnection? connection)
        {
        }
    }

    [Fact]
    public async Task CommitAsync_ClearsCachedNormalizedConnectionString()
    {
        using var mySql = new AsyncCleanupTransactionMySql();
        mySql.SeedTransactionState("Server=host;Database=database;User ID=user;Password=password");

        Assert.NotNull(TransactionConnectionStringField.GetValue(mySql));

        await mySql.CommitAsync();

        Assert.Null(TransactionConnectionStringField.GetValue(mySql));
    }

    [Fact]
    public async Task RollbackAsync_ClearsCachedNormalizedConnectionString()
    {
        using var mySql = new AsyncCleanupTransactionMySql();
        mySql.SeedTransactionState("Server=host;Database=database;User ID=user;Password=password");

        Assert.NotNull(TransactionConnectionStringField.GetValue(mySql));

        await mySql.RollbackAsync();

        Assert.Null(TransactionConnectionStringField.GetValue(mySql));
    }

    private sealed class AsyncDisposeTrackingMySql : DBAClientX.MySql
    {
        public int AsyncRollbackCalls { get; private set; }
        public int AsyncTransactionDisposals { get; private set; }
        public int AsyncConnectionDisposals { get; private set; }

        public void SeedActiveTransaction()
        {
            TransactionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(MySqlTransaction)));
            TransactionConnectionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(MySqlConnection)));
        }

        protected override ValueTask DisposeDbTransactionAsync(MySqlTransaction transaction)
        {
            AsyncTransactionDisposals++;
            return default;
        }

        protected override Task TryRollbackDbTransactionOnDisposeAsync(MySqlTransaction? transaction)
        {
            AsyncRollbackCalls++;
            return Task.CompletedTask;
        }

        protected override ValueTask DisposeConnectionAsync(MySqlConnection connection)
        {
            AsyncConnectionDisposals++;
            return default;
        }

        protected override void TryRollbackDbTransactionOnDispose(MySqlTransaction? transaction)
            => throw new InvalidOperationException("Synchronous rollback should not be used by DisposeAsync.");

        protected override void DisposeDbTransaction(MySqlTransaction transaction)
            => throw new InvalidOperationException("Synchronous transaction disposal should not be used by DisposeAsync.");

        protected override void DisposeConnection(MySqlConnection connection)
            => throw new InvalidOperationException("Synchronous connection disposal should not be used by DisposeAsync.");
    }

    [Fact]
    public async Task DisposeAsync_WithActiveTransaction_UsesAsyncCleanupAndClearsState()
    {
        var mySql = new AsyncDisposeTrackingMySql();
        mySql.SeedActiveTransaction();

        await mySql.DisposeAsync();
        await mySql.DisposeAsync();

        Assert.False(mySql.IsInTransaction);
        Assert.Equal(1, mySql.AsyncRollbackCalls);
        Assert.Equal(1, mySql.AsyncTransactionDisposals);
        Assert.Equal(1, mySql.AsyncConnectionDisposals);
    }
}
