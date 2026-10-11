using System.Data.Common;
using DBAClientX;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

public class SQLiteAsyncSessionTransactionModeTests
{
    [Fact]
    public async Task DeferredReadKeepsSnapshotWhileAnExistingWriterCommits()
    {
        string path = Path.Join(Path.GetTempPath(), $"{Guid.NewGuid():N}.db");
        try
        {
            using var sqlite = CreateClient();
            await using SQLiteAsyncSession reader = await sqlite.OpenSessionAsync(path);
            await reader.ExecuteNonQueryAsync("CREATE TABLE items (name TEXT NOT NULL); INSERT INTO items VALUES ('before');");
            using DbConnection writer = sqlite.OpenDbConnection(path);
            using DbTransaction write = sqlite.BeginDbTransaction(writer);
            using DbCommand insert = writer.CreateCommand();
            insert.Transaction = write;
            insert.CommandText = "INSERT INTO items VALUES ('during-snapshot');";
            Assert.Equal(1, insert.ExecuteNonQuery());

            long count = await reader.RunInTransactionAsync(SQLiteTransactionMode.Deferred, async (snapshot, token) =>
            {
                Assert.Equal(1L, await snapshot.ExecuteScalarAsync("SELECT COUNT(*) FROM items;", cancellationToken: token));
                write.Commit();
                return Convert.ToInt64(await snapshot.ExecuteScalarAsync("SELECT COUNT(*) FROM items;", cancellationToken: token));
            });

            Assert.Equal(1L, count);
            Assert.Equal(2L, await reader.ExecuteScalarAsync("SELECT COUNT(*) FROM items;"));
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task ExistingOverloadReservesWriteIntentBeforeTheFirstCommand()
    {
        string path = Path.Join(Path.GetTempPath(), $"{Guid.NewGuid():N}.db");
        try
        {
            using var sqlite = CreateClient();
            await using SQLiteAsyncSession session = await sqlite.OpenSessionAsync(path);
            await session.ExecuteNonQueryAsync("CREATE TABLE items (name TEXT NOT NULL);");
            using DbConnection competingWriter = sqlite.OpenDbConnection(path);

            await session.RunInTransactionAsync((_, _) =>
            {
                SqliteException error = Assert.Throws<SqliteException>(() => sqlite.BeginDbTransaction(competingWriter));
                Assert.Equal(5, error.SqliteErrorCode);
                return Task.CompletedTask;
            }, default);

            int result = await session.RunInTransactionAsync((_, _) => Task.FromResult(42), default);
            Assert.Equal(42, result);

            using DbTransaction afterCommit = sqlite.BeginDbTransaction(competingWriter);
            afterCommit.Rollback();
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Theory]
    [InlineData(SQLiteTransactionMode.Immediate)]
    [InlineData(SQLiteTransactionMode.Deferred)]
    public async Task CancelledCallbackRollsBackAndLeavesSessionReusable(SQLiteTransactionMode mode)
    {
        string path = Path.Join(Path.GetTempPath(), $"{Guid.NewGuid():N}.db");
        try
        {
            using var sqlite = CreateClient();
            await using SQLiteAsyncSession session = await sqlite.OpenSessionAsync(path);
            await session.ExecuteNonQueryAsync("CREATE TABLE items (name TEXT NOT NULL);");
            using var cancellation = new CancellationTokenSource();

            OperationCanceledException error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                session.RunInTransactionAsync(mode, async (transaction, token) =>
                {
                    await transaction.ExecuteNonQueryAsync("INSERT INTO items VALUES ('rolled-back');", cancellationToken: token);
                    cancellation.Cancel();
                    token.ThrowIfCancellationRequested();
                }, cancellation.Token));

            Assert.Equal(cancellation.Token, error.CancellationToken);
            await session.ExecuteNonQueryAsync("INSERT INTO items VALUES ('retained');");
            Assert.Equal("retained", await session.ExecuteScalarAsync("SELECT name FROM items;"));
            Assert.Equal(1L, await session.ExecuteScalarAsync("SELECT COUNT(*) FROM items;"));
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task DeferredTransactionObservesCancellationBeforeInvokingCallback()
    {
        string path = Path.Join(Path.GetTempPath(), $"{Guid.NewGuid():N}.db");
        try
        {
            using var sqlite = CreateClient();
            await using SQLiteAsyncSession session = await sqlite.OpenSessionAsync(path);
            using var cancellation = new CancellationTokenSource();
            cancellation.Cancel();
            bool invoked = false;

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                session.RunInTransactionAsync(SQLiteTransactionMode.Deferred, (_, _) =>
                {
                    invoked = true;
                    return Task.FromResult(42);
                }, cancellation.Token));

            Assert.False(invoked);
            Assert.Equal(42L, await session.ExecuteScalarAsync("SELECT 42;"));
        }
        finally
        {
            Cleanup(path);
        }
    }

    private static SQLite CreateClient() => new()
    {
        CommandTimeout = 1,
        BusyTimeoutMs = 250,
        ConnectionOptions = new SQLiteConnectionOptions { EnableWriteAheadLogging = true, Pooling = false }
    };

    private static void Cleanup(string path)
    {
        foreach (string file in new[] { path, path + "-wal", path + "-shm" })
        {
            if (File.Exists(file)) File.Delete(file);
        }
    }
}
