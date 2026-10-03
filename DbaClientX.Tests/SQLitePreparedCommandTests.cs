using System;
using System.IO;
using System.Threading.Tasks;
using DBAClientX;

namespace DbaClientX.Tests;

[Collection(SqlitePoolCleanupCollection.Name)]
public class SQLitePreparedCommandTests
{
    private enum Status
    {
        Ok = 0,
        Failed = 4
    }

    [Fact]
    public void PrepareInsert_InsideTransaction_WritesEveryRowAtomically()
    {
        string path = NewDatabasePath();
        try
        {
            using var sqlite = new SQLite();
            sqlite.ExecuteNonQuery(path, "CREATE TABLE items (id INTEGER NOT NULL, name TEXT NULL, status INTEGER NOT NULL);");
            using SQLiteSession session = sqlite.OpenSession(path);

            session.RunInTransaction(transaction =>
            {
                using SQLitePreparedCommand insert = transaction.PrepareInsert("items", "id", "name", "status");
                for (int index = 0; index < 20_000; index++)
                {
                    insert.ExecuteNonQuery(index, index % 2 == 0 ? $"item {index}" : null, index % 10 == 0 ? Status.Failed : Status.Ok);
                }
            });

            Assert.Equal(20_000L, session.ExecuteScalar("SELECT COUNT(*) FROM items;"));
            Assert.Equal(10_000L, session.ExecuteScalar("SELECT COUNT(*) FROM items WHERE name IS NULL;"));
            Assert.Equal(2_000L, session.ExecuteScalar("SELECT COUNT(*) FROM items WHERE status = 4;"));
            Assert.Equal("integer", session.ExecuteScalar("SELECT typeof(status) FROM items LIMIT 1;"));
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public void PrepareInsert_RollsBackWithTheTransaction()
    {
        string path = NewDatabasePath();
        try
        {
            using var sqlite = new SQLite();
            sqlite.ExecuteNonQuery(path, "CREATE TABLE items (id INTEGER NOT NULL);");
            using SQLiteSession session = sqlite.OpenSession(path);

            Assert.Throws<InvalidOperationException>(() => session.RunInTransaction(transaction =>
            {
                using SQLitePreparedCommand insert = transaction.PrepareInsert("items", "id");
                insert.ExecuteNonQuery(1);
                insert.ExecuteNonQuery(2);
                throw new InvalidOperationException("stop");
            }));

            Assert.Equal(0L, session.ExecuteScalar("SELECT COUNT(*) FROM items;"));
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public void Prepare_NamedParameters_ReturnsScalarPerExecution()
    {
        string path = NewDatabasePath();
        try
        {
            using var sqlite = new SQLite();
            sqlite.ExecuteNonQuery(path, "CREATE TABLE runs (run_key INTEGER PRIMARY KEY, run_id TEXT NOT NULL UNIQUE);");
            using SQLiteSession session = sqlite.OpenSession(path);
            using SQLitePreparedCommand insert = session.Prepare(
                "INSERT INTO runs (run_id) VALUES ($runId); SELECT last_insert_rowid();",
                "$runId");

            Assert.Equal(1, insert.ParameterCount);
            Assert.Equal(1L, insert.ExecuteScalar("first"));
            Assert.Equal(2L, insert.ExecuteScalar("second"));
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public void PrepareInsert_QuotesIdentifiers()
    {
        string path = NewDatabasePath();
        try
        {
            using var sqlite = new SQLite();
            sqlite.ExecuteNonQuery(path, "CREATE TABLE \"order items\" (\"select\" TEXT NOT NULL, \"group\" INTEGER NOT NULL);");
            using SQLiteSession session = sqlite.OpenSession(path);
            using (SQLitePreparedCommand insert = session.PrepareInsert("order items", "select", "group"))
            {
                insert.ExecuteNonQuery("a", 1);
            }

            Assert.Equal("a", session.ExecuteScalar("SELECT \"select\" FROM \"order items\";"));
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public void Execute_RejectsWrongValueCountAndDisposedUse()
    {
        string path = NewDatabasePath();
        try
        {
            using var sqlite = new SQLite();
            sqlite.ExecuteNonQuery(path, "CREATE TABLE items (id INTEGER NOT NULL, name TEXT NOT NULL);");
            using SQLiteSession session = sqlite.OpenSession(path);
            SQLitePreparedCommand insert = session.PrepareInsert("items", "id", "name");

            Assert.Throws<ArgumentException>(() => insert.ExecuteNonQuery(1));
            insert.Dispose();
            Assert.Throws<ObjectDisposedException>(() => insert.ExecuteNonQuery(1, "a"));
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public void PrepareAndExecute_WrapProviderErrors()
    {
        string path = NewDatabasePath();
        try
        {
            using var sqlite = new SQLite();
            sqlite.ExecuteNonQuery(path, "CREATE TABLE items (id INTEGER NOT NULL UNIQUE);");
            using SQLiteSession session = sqlite.OpenSession(path);

            Assert.Throws<DbaQueryExecutionException>(() => session.Prepare("INSERT INTO missing (id) VALUES ($id);", "$id"));
            using SQLitePreparedCommand insert = session.PrepareInsert("items", "id");
            insert.ExecuteNonQuery(1);
            Assert.Throws<DbaQueryExecutionException>(() => insert.ExecuteNonQuery(1));
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task AsyncSession_PrepareInsert_WritesRowsInsideTransaction()
    {
        string path = NewDatabasePath();
        try
        {
            using var sqlite = new SQLite();
            sqlite.ExecuteNonQuery(path, "CREATE TABLE items (id INTEGER NOT NULL, name TEXT NOT NULL);");
            await using SQLiteAsyncSession session = await sqlite.OpenSessionAsync(path);

            await session.RunInTransactionAsync(async (transaction, token) =>
            {
                using SQLitePreparedCommand insert = transaction.PrepareInsert("items", "id", "name");
                for (int index = 0; index < 500; index++)
                {
                    await insert.ExecuteNonQueryAsync(new object?[] { index, "row" }, token);
                }
            });

            Assert.Equal(500L, await session.ExecuteScalarAsync("SELECT COUNT(*) FROM items;"));
        }
        finally
        {
            Cleanup(path);
        }
    }

    private static string NewDatabasePath()
        => Path.Join(Path.GetTempPath(), Path.GetFileName($"{Guid.NewGuid():N}.db"));

    private static void Cleanup(string path)
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        foreach (string file in new[] { path, path + "-wal", path + "-shm" })
        {
            if (File.Exists(file))
            {
                File.Delete(file);
            }
        }
    }
}
