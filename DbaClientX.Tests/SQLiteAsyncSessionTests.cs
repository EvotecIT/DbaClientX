using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Reflection;
using System.Threading;
using DBAClientX;

namespace DbaClientX.Tests;

public class SQLiteAsyncSessionTests
{
    [Fact]
    public async Task OpenSessionAsync_ExecutesMappedQueriesOnOneConnection()
    {
        string path = Path.Join(Path.GetTempPath(), $"{Guid.NewGuid():N}.db");
        try
        {
            using var sqlite = new SQLite { BusyTimeoutMs = 15000, MaxRetryAttempts = 5 };
            await using SQLiteAsyncSession session = await sqlite.OpenSessionAsync(path);
            await session.ExecuteNonQueryAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL);");
            await session.ExecuteNonQueryAsync(
                "INSERT INTO items (name) VALUES ($name);",
                new Dictionary<string, object?> { ["$name"] = "one" });

            IReadOnlyList<string> rows = await session.QueryAsListAsync(
                "SELECT name FROM items ORDER BY id;",
                static record => record.GetString(0));

            Assert.Equal(["one"], rows);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task RunInTransactionAsync_RollsBackWhenOperationFails()
    {
        string path = Path.Join(Path.GetTempPath(), $"{Guid.NewGuid():N}.db");
        try
        {
            using var sqlite = new SQLite();
            await using SQLiteAsyncSession session = await sqlite.OpenSessionAsync(path);
            await session.ExecuteNonQueryAsync("CREATE TABLE items (name TEXT NOT NULL);");

            await Assert.ThrowsAsync<InvalidOperationException>(() => session.RunInTransactionAsync<int>(async (tx, token) =>
            {
                await tx.ExecuteNonQueryAsync("INSERT INTO items (name) VALUES ('temp');", cancellationToken: token);
                throw new InvalidOperationException("stop");
            }));

            object? count = await session.ExecuteScalarAsync("SELECT COUNT(*) FROM items;");
            Assert.Equal(0L, count);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task RunInTransactionAsync_PreservesOperationAndRollbackFailures()
    {
        string path = Path.Join(Path.GetTempPath(), $"{Guid.NewGuid():N}.db");
        try
        {
            using var sqlite = new SQLite();
            await using SQLiteAsyncSession session = await sqlite.OpenSessionAsync(path);

            AggregateException error = await Assert.ThrowsAsync<AggregateException>(() =>
                session.RunInTransactionAsync<int>(async (transaction, token) =>
                {
                    var field = typeof(SQLiteAsyncSession).GetField("_transaction", BindingFlags.Instance | BindingFlags.NonPublic)!;
                    var providerTransaction = (DbTransaction)field.GetValue(transaction)!;
                    await providerTransaction.DisposeAsync();
                    throw new InvalidOperationException("operation failed");
                }));

            Assert.Collection(
                error.InnerExceptions,
                original => Assert.Equal("operation failed", original.Message),
                rollback => Assert.NotNull(rollback));
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task OpenSessionAsync_ObservesCancellation()
    {
        using var sqlite = new SQLite();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            sqlite.OpenSessionAsync(Path.Join(Path.GetTempPath(), $"{Guid.NewGuid():N}.db"), cancellation.Token));
    }

    private static void Cleanup(string path)
    {
        TryDelete(path);
        TryDelete(path + "-wal");
        TryDelete(path + "-shm");
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }
}
