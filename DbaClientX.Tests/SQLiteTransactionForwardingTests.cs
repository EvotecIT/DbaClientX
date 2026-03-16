using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace DbaClientX.Tests;

public class SQLiteTransactionForwardingTests
{
    private class TestSQLite : DBAClientX.SQLite
    {
        public bool SyncCalled { get; private set; }
        public bool AsyncCalled { get; private set; }
        public IsolationLevel? SyncIsolationLevel { get; private set; }
        public IsolationLevel? AsyncIsolationLevel { get; private set; }

        public override void BeginTransaction(string database)
        {
            SyncCalled = true;
        }

        public override void BeginTransaction(string database, IsolationLevel isolationLevel)
        {
            SyncCalled = true;
            SyncIsolationLevel = isolationLevel;
        }

        public override Task BeginTransactionAsync(string database, CancellationToken cancellationToken = default)
        {
            AsyncCalled = true;
            return Task.CompletedTask;
        }

        public override Task BeginTransactionAsync(string database, IsolationLevel isolationLevel, CancellationToken cancellationToken = default)
        {
            AsyncCalled = true;
            AsyncIsolationLevel = isolationLevel;
            return Task.CompletedTask;
        }
    }

    [Fact]
    public void BeginTransaction_WithIsolationLevel_ForwardsToBeginTransaction()
    {
        using var sqlite = new TestSQLite();
        sqlite.BeginTransaction(":memory:", IsolationLevel.Serializable);
        Assert.True(sqlite.SyncCalled);
        Assert.Equal(IsolationLevel.Serializable, sqlite.SyncIsolationLevel);
    }

    [Fact]
    public async Task BeginTransactionAsync_WithIsolationLevel_ForwardsToBeginTransaction()
    {
        using var sqlite = new TestSQLite();
        await sqlite.BeginTransactionAsync(":memory:", IsolationLevel.Serializable);
        Assert.True(sqlite.AsyncCalled);
        Assert.Equal(IsolationLevel.Serializable, sqlite.AsyncIsolationLevel);
    }
}
