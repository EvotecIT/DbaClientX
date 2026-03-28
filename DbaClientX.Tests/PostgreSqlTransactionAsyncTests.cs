using System.Collections.Generic;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Data;
using Npgsql;
using NpgsqlTypes;
using Xunit;

namespace DbaClientX.Tests;

public class PostgreSqlTransactionAsyncTests
{
    private static readonly FieldInfo TransactionField = typeof(DBAClientX.PostgreSql)
        .GetField("_transaction", BindingFlags.Instance | BindingFlags.NonPublic)!;
    private static readonly FieldInfo TransactionConnectionField = typeof(DBAClientX.PostgreSql)
        .GetField("_transactionConnection", BindingFlags.Instance | BindingFlags.NonPublic)!;

    private class FakeNpgsqlConnection
    {
        public bool BeginCalled { get; private set; }
        public IsolationLevel? Level { get; private set; }

        public Task<FakeNpgsqlTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
            => BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);

        public async Task<FakeNpgsqlTransaction> BeginTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken = default)
        {
            BeginCalled = true;
            Level = isolationLevel;
            await Task.Yield();
            return new FakeNpgsqlTransaction(this);
        }
    }

    private class FakeNpgsqlTransaction
    {
        private readonly FakeNpgsqlConnection _connection;
        public bool CommitCalled { get; private set; }
        public bool RollbackCalled { get; private set; }

        public FakeNpgsqlTransaction(FakeNpgsqlConnection connection)
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

    private class TestPostgreSql : DBAClientX.PostgreSql
    {
        private readonly object _syncRoot = new();
        public FakeNpgsqlConnection? Connection { get; private set; }
        public FakeNpgsqlTransaction? Transaction { get; private set; }

        public override async Task BeginTransactionAsync(string host, string database, string username, string password, CancellationToken cancellationToken = default)
        {
            lock (_syncRoot)
            {
                if (Transaction != null)
                {
                    throw new DBAClientX.DbaTransactionException("Transaction already started.");
                }
            }

            var connection = new FakeNpgsqlConnection();
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

            var connection = new FakeNpgsqlConnection();
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

        public override Task<object?> QueryAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, NpgsqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
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
        using var pg = new TestPostgreSql();
        await pg.BeginTransactionAsync("h", "d", "u", "p");
        Assert.NotNull(pg.Connection);
        Assert.True(pg.Connection!.BeginCalled);
        Assert.NotNull(pg.Transaction);
    }

    [Fact]
    public async Task BeginTransactionAsync_WhenCalledConcurrently_Throws()
    {
        using var pg = new TestPostgreSql();

        var tasks = new[]
        {
            Task.Run(() => pg.BeginTransactionAsync("h", "d", "u", "p")),
            Task.Run(() => pg.BeginTransactionAsync("h", "d", "u", "p"))
        };

        await Assert.ThrowsAsync<DBAClientX.DbaTransactionException>(() => Task.WhenAll(tasks));
        Assert.NotNull(pg.Transaction);
    }

    [Fact]
    public async Task CommitAsync_CallsCommitOnTransaction()
    {
        using var pg = new TestPostgreSql();
        await pg.BeginTransactionAsync("h", "d", "u", "p");
        var txn = pg.Transaction!;
        await pg.CommitAsync();
        Assert.True(txn.CommitCalled);
        Assert.Null(pg.Transaction);
    }

    [Fact]
    public async Task RollbackAsync_CallsRollbackOnTransaction()
    {
        using var pg = new TestPostgreSql();
        await pg.BeginTransactionAsync("h", "d", "u", "p");
        var txn = pg.Transaction!;
        await pg.RollbackAsync();
        Assert.True(txn.RollbackCalled);
        Assert.Null(pg.Transaction);
    }

    [Fact]
    public async Task BeginTransactionAsync_WithIsolationLevel_PassesIsolationLevel()
    {
        using var pg = new TestPostgreSql();
        await pg.BeginTransactionAsync("h", "d", "u", "p", IsolationLevel.Serializable);
        Assert.NotNull(pg.Connection);
        Assert.Equal(IsolationLevel.Serializable, pg.Connection!.Level);
    }

    [Fact]
    public async Task RunInTransactionAsync_CommitsAndReturnsResult()
    {
        using var pg = new TestPostgreSql();

        var result = await pg.RunInTransactionAsync("h", "d", "u", "p", async (client, token) =>
        {
            await client.QueryAsync("h", "d", "u", "p", "q", useTransaction: true, cancellationToken: token);
            return 42;
        });

        Assert.Equal(42, result);
        Assert.Null(pg.Transaction);
    }

    [Fact]
    public async Task RunInTransactionAsync_WhenOperationFails_RollsBackAndRethrows()
    {
        using var pg = new TestPostgreSql();
        FakeNpgsqlTransaction? txn = null;

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            pg.RunInTransactionAsync("h", "d", "u", "p", async (_, _) =>
            {
                txn = pg.Transaction;
                await Task.Yield();
                throw new InvalidOperationException("boom");
            }));

        Assert.Equal("boom", ex.Message);
        Assert.NotNull(txn);
        Assert.True(txn!.RollbackCalled);
        Assert.Null(pg.Transaction);
    }

    private class OpenFailureTransactionPostgreSql : DBAClientX.PostgreSql
    {
        public int DisposeCalls { get; private set; }

        protected override Task OpenConnectionAsync(Npgsql.NpgsqlConnection connection, CancellationToken cancellationToken)
            => Task.FromException(new InvalidOperationException("boom"));

        protected override void DisposeConnection(Npgsql.NpgsqlConnection connection)
            => DisposeCalls++;

        protected override ValueTask DisposeConnectionAsync(Npgsql.NpgsqlConnection connection)
        {
            DisposeCalls++;
            return default;
        }
    }

    [Fact]
    public async Task BeginTransactionAsync_WhenOpenFails_DisposesConnectionAndResetsState()
    {
        using var pg = new OpenFailureTransactionPostgreSql();

        await Assert.ThrowsAsync<InvalidOperationException>(() => pg.BeginTransactionAsync("h", "d", "u", "p"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => pg.BeginTransactionAsync("h", "d", "u", "p"));

        Assert.False(pg.IsInTransaction);
        Assert.Equal(2, pg.DisposeCalls);
    }

    private class BeginFailureTransactionPostgreSql : DBAClientX.PostgreSql
    {
        public int DisposeCalls { get; private set; }

        protected override Task OpenConnectionAsync(Npgsql.NpgsqlConnection connection, CancellationToken cancellationToken)
            => Task.CompletedTask;

        protected override Task<Npgsql.NpgsqlTransaction> BeginDbTransactionAsync(Npgsql.NpgsqlConnection connection, IsolationLevel isolationLevel, CancellationToken cancellationToken)
            => Task.FromException<Npgsql.NpgsqlTransaction>(new InvalidOperationException("boom"));

        protected override void DisposeConnection(Npgsql.NpgsqlConnection connection)
            => DisposeCalls++;

        protected override ValueTask DisposeConnectionAsync(Npgsql.NpgsqlConnection connection)
        {
            DisposeCalls++;
            return default;
        }
    }

    [Fact]
    public async Task BeginTransactionAsync_WhenBeginDbTransactionFails_DisposesConnectionAndResetsState()
    {
        using var pg = new BeginFailureTransactionPostgreSql();

        await Assert.ThrowsAsync<InvalidOperationException>(() => pg.BeginTransactionAsync("h", "d", "u", "p"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => pg.BeginTransactionAsync("h", "d", "u", "p"));

        Assert.False(pg.IsInTransaction);
        Assert.Equal(2, pg.DisposeCalls);
    }

    private sealed class AsyncDisposeTrackingPostgreSql : DBAClientX.PostgreSql
    {
        public int AsyncRollbackCalls { get; private set; }
        public int AsyncTransactionDisposals { get; private set; }
        public int AsyncConnectionDisposals { get; private set; }

        public void SeedActiveTransaction()
        {
            TransactionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(NpgsqlTransaction)));
            TransactionConnectionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(NpgsqlConnection)));
        }

        protected override ValueTask DisposeDbTransactionAsync(NpgsqlTransaction transaction)
        {
            AsyncTransactionDisposals++;
            return default;
        }

        protected override Task TryRollbackDbTransactionOnDisposeAsync(NpgsqlTransaction? transaction)
        {
            AsyncRollbackCalls++;
            return Task.CompletedTask;
        }

        protected override ValueTask DisposeConnectionAsync(NpgsqlConnection connection)
        {
            AsyncConnectionDisposals++;
            return default;
        }

        protected override void TryRollbackDbTransactionOnDispose(NpgsqlTransaction? transaction)
            => throw new InvalidOperationException("Synchronous rollback should not be used by DisposeAsync.");

        protected override void DisposeDbTransaction(NpgsqlTransaction transaction)
            => throw new InvalidOperationException("Synchronous transaction disposal should not be used by DisposeAsync.");

        protected override void DisposeConnection(NpgsqlConnection connection)
            => throw new InvalidOperationException("Synchronous connection disposal should not be used by DisposeAsync.");
    }

    [Fact]
    public async Task DisposeAsync_WithActiveTransaction_UsesAsyncCleanupAndClearsState()
    {
        var pg = new AsyncDisposeTrackingPostgreSql();
        pg.SeedActiveTransaction();

        await pg.DisposeAsync();
        await pg.DisposeAsync();

        Assert.False(pg.IsInTransaction);
        Assert.Equal(1, pg.AsyncRollbackCalls);
        Assert.Equal(1, pg.AsyncTransactionDisposals);
        Assert.Equal(1, pg.AsyncConnectionDisposals);
    }
}
