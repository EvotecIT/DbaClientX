using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Data;
using Microsoft.Data.Sqlite;
using Xunit;

namespace DbaClientX.Tests;

public class SQLiteTransactionAsyncTests
{
    private class FakeSqliteConnection
    {
        public bool BeginCalled { get; private set; }
        public async Task<FakeSqliteTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
        {
            BeginCalled = true;
            await Task.Yield();
            return new FakeSqliteTransaction(this);
        }
    }

    private class FakeSqliteTransaction
    {
        private readonly FakeSqliteConnection _connection;
        public bool CommitCalled { get; private set; }
        public bool RollbackCalled { get; private set; }

        public FakeSqliteTransaction(FakeSqliteConnection connection)
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

    private class TestSQLite : DBAClientX.SQLite
    {
        private readonly object _syncRoot = new();
        public FakeSqliteConnection? Connection { get; private set; }
        public FakeSqliteTransaction? Transaction { get; private set; }

        public override async Task BeginTransactionAsync(string database, CancellationToken cancellationToken = default)
        {
            lock (_syncRoot)
            {
                if (Transaction != null)
                {
                    throw new DBAClientX.DbaTransactionException("Transaction already started.");
                }
            }

            var connection = new FakeSqliteConnection();
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

        public override Task<object?> QueryAsync(string database, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, SqliteType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
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
        using var sqlite = new TestSQLite();
        await sqlite.BeginTransactionAsync(":memory:");
        Assert.NotNull(sqlite.Connection);
        Assert.True(sqlite.Connection!.BeginCalled);
        Assert.NotNull(sqlite.Transaction);
    }

    [Fact]
    public async Task BeginTransactionAsync_WhenCalledConcurrently_Throws()
    {
        using var sqlite = new TestSQLite();

        var tasks = new[]
        {
            Task.Run(() => sqlite.BeginTransactionAsync(":memory:")),
            Task.Run(() => sqlite.BeginTransactionAsync(":memory:"))
        };

        await Assert.ThrowsAsync<DBAClientX.DbaTransactionException>(() => Task.WhenAll(tasks));
        Assert.NotNull(sqlite.Transaction);
    }

    [Fact]
    public async Task BeginTransactionAsync_OnConcreteClient_ReservesInitialization()
    {
        using var sqlite = new DBAClientX.SQLite();

        var first = sqlite.BeginTransactionAsync(":memory:");
        await Assert.ThrowsAsync<DBAClientX.DbaTransactionException>(async () => await sqlite.BeginTransactionAsync(":memory:"));

        await first;
        Assert.True(sqlite.IsInTransaction);

        await sqlite.RollbackAsync();
        Assert.False(sqlite.IsInTransaction);
    }

    [Fact]
    public async Task CommitAsync_CallsCommitOnTransaction()
    {
        using var sqlite = new TestSQLite();
        await sqlite.BeginTransactionAsync(":memory:");
        var txn = sqlite.Transaction!;
        await sqlite.CommitAsync();
        Assert.True(txn.CommitCalled);
        Assert.Null(sqlite.Transaction);
    }

    [Fact]
    public async Task RollbackAsync_CallsRollbackOnTransaction()
    {
        using var sqlite = new TestSQLite();
        await sqlite.BeginTransactionAsync(":memory:");
        var txn = sqlite.Transaction!;
        await sqlite.RollbackAsync();
        Assert.True(txn.RollbackCalled);
        Assert.Null(sqlite.Transaction);
    }
}
