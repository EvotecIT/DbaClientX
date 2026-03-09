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

    private class OpenFailureTransactionMySql : DBAClientX.MySql
    {
        public int DisposeCalls { get; private set; }

        protected override Task OpenConnectionAsync(MySqlConnection connection, CancellationToken cancellationToken)
            => Task.FromException(new InvalidOperationException("boom"));

        protected override void DisposeConnection(MySqlConnection connection)
            => DisposeCalls++;
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
}
