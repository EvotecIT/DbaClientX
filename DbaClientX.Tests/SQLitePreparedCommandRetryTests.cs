using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using DBAClientX;

namespace DbaClientX.Tests;

[Collection(SqlitePoolCleanupCollection.Name)]
public class SQLitePreparedCommandRetryTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task PreparedExecution_RetriesBusyStatementOnTheSameConnection(bool asynchronous, bool scalar)
    {
        string path = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid():N}.db");
        try
        {
            using var locker = new SQLite();
            locker.ExecuteNonQuery(path, "CREATE TABLE items (id INTEGER PRIMARY KEY);");
            using var sqlite = new UnlockOnBusySQLite
            {
                CommandTimeout = 1,
                BusyTimeoutMs = 1,
                MaxRetryAttempts = 2,
                RetryDelay = TimeSpan.Zero,
                RetryNonQueryOperations = true,
                CommandRetryMode = CommandRetryMode.ReplaySafe,
                Unlock = locker.Rollback
            };
            using SQLiteSession session = sqlite.OpenSession(path);
            using SQLitePreparedCommand command = session.Prepare(
                scalar ? "INSERT INTO items (id) VALUES ($id) RETURNING id;" : "INSERT INTO items (id) VALUES ($id);", "$id");
            locker.BeginTransaction(path);
            locker.ExecuteNonQuery(path, "INSERT INTO items (id) VALUES (99);", useTransaction: true);

            if (asynchronous)
            {
                if (scalar) Assert.Equal(1L, await command.ExecuteScalarAsync(new object?[] { 1 }));
                else Assert.Equal(1, await command.ExecuteNonQueryAsync(new object?[] { 1 }));
            }
            else
            {
                if (scalar) Assert.Equal(1L, command.ExecuteScalar(1));
                else Assert.Equal(1, command.ExecuteNonQuery(1));
            }

            Assert.Equal(1, sqlite.BusyFailures);
            Assert.Equal(1L, session.ExecuteScalar("SELECT COUNT(*) FROM items;"));
            using var cancellation = new CancellationTokenSource();
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                command.ExecuteNonQueryAsync(new object?[] { 2 }, cancellation.Token));
            Assert.Equal(1L, session.ExecuteScalar("SELECT COUNT(*) FROM items;"));
        }
        finally { Cleanup(path); }
    }

    [Fact]
    public void PreparedNonQuery_DoesNotRetryWritesByDefault()
    {
        string path = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid():N}.db");
        try
        {
            using var locker = new SQLite();
            locker.ExecuteNonQuery(path, "CREATE TABLE items (id INTEGER PRIMARY KEY);");
            using var sqlite = new UnlockOnBusySQLite
            {
                CommandTimeout = 1, BusyTimeoutMs = 1, MaxRetryAttempts = 3,
                RetryDelay = TimeSpan.Zero, Unlock = locker.Rollback
            };
            using SQLiteSession session = sqlite.OpenSession(path);
            using SQLitePreparedCommand command = session.PrepareInsert("items", "id");
            locker.BeginTransaction(path);
            locker.ExecuteNonQuery(path, "INSERT INTO items (id) VALUES (99);", useTransaction: true);

            Assert.False(sqlite.RetryNonQueryOperations);
            var failure = Assert.Throws<DbaQueryExecutionException>(() => command.ExecuteNonQuery(1));
            Assert.True(failure.ProviderErrorCode is 5 or 6);
            Assert.Equal(0, sqlite.BusyFailures);
            locker.Rollback();
            Assert.Equal(0L, session.ExecuteScalar("SELECT COUNT(*) FROM items;"));
        }
        finally { Cleanup(path); }
    }

    private sealed class UnlockOnBusySQLite : SQLite
    {
        public Action Unlock { get; init; } = null!;
        public int BusyFailures { get; private set; }
        protected override bool IsTransient(Exception exception)
        {
            bool transient = base.IsTransient(exception);
            if (transient) { BusyFailures++; Unlock(); }
            return transient;
        }
    }

    private static void Cleanup(string path)
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        foreach (string file in new[] { path, path + "-wal", path + "-shm" })
            if (File.Exists(file)) File.Delete(file);
    }
}
