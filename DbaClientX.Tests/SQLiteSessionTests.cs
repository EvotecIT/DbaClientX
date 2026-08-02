using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using DBAClientX;

namespace DbaClientX.Tests;

public class SQLiteSessionTests
{
    [Fact]
    public void OpenDbConnection_AppliesManagedOptions()
    {
        string path = Path.Join(Path.GetTempPath(), Path.GetFileName($"{Guid.NewGuid():N}.db"));
        try
        {
            using var sqlite = new SQLite();
            using DbConnection connection = sqlite.OpenDbConnection(path, new SQLiteConnectionOptions
            {
                BusyTimeoutMs = 4321,
                EnableForeignKeys = true,
                EnableWriteAheadLogging = true
            });

            Assert.Equal(4321L, ReadPragma(connection, "busy_timeout"));
            Assert.Equal(1L, ReadPragma(connection, "foreign_keys"));
            Assert.Equal("wal", ReadPragma(connection, "journal_mode"));
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public void BeginDbTransaction_DeferredSnapshotAllowsConcurrentWriter()
    {
        string path = Path.Join(Path.GetTempPath(), Path.GetFileName($"{Guid.NewGuid():N}.db"));
        try
        {
            using var sqlite = new SQLite();
            sqlite.ExecuteNonQuery(path, "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL);");
            sqlite.ExecuteNonQuery(path, "INSERT INTO items (name) VALUES ('before');");
            var options = new SQLiteConnectionOptions
            {
                BusyTimeoutMs = 250,
                EnableWriteAheadLogging = true
            };
            using DbConnection reader = sqlite.OpenDbConnection(path, options);
            using DbConnection writer = sqlite.OpenDbConnection(path, options);
            using DbTransaction snapshot = sqlite.BeginDbTransaction(reader, SQLiteTransactionMode.Deferred);

            Assert.Equal(1L, ReadCount(reader, snapshot));

            using (DbTransaction write = sqlite.BeginDbTransaction(writer))
            {
                using DbCommand insert = writer.CreateCommand();
                insert.Transaction = write;
                insert.CommandText = "INSERT INTO items (name) VALUES ('during-snapshot');";
                Assert.Equal(1, insert.ExecuteNonQuery());
                write.Commit();
            }

            Assert.Equal(1L, ReadCount(reader, snapshot));
            snapshot.Commit();
            Assert.Equal(2L, ReadCount(reader, transaction: null));
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public void OpenSession_AppliesConfiguredBusyTimeout()
    {
        string path = Path.Join(Path.GetTempPath(), Path.GetFileName($"{Guid.NewGuid():N}.db"));
        try
        {
            using var sqlite = new SQLite { BusyTimeoutMs = 7500 };
            using SQLiteSession session = sqlite.OpenSession(path);

            Assert.Equal(7500L, Convert.ToInt64(session.ExecuteScalar("PRAGMA busy_timeout;")));
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task QueryAsync_AppliesConfiguredBusyTimeout()
    {
        string path = Path.Join(Path.GetTempPath(), Path.GetFileName($"{Guid.NewGuid():N}.db"));
        try
        {
            using var sqlite = new SQLite
            {
                BusyTimeoutMs = 7500,
                ReturnType = ReturnType.DataTable
            };

            object? result = await sqlite.QueryAsync(path, "PRAGMA busy_timeout;");
            DataTable table = Assert.IsType<DataTable>(result);

            Assert.Equal(7500L, Convert.ToInt64(table.Rows[0][0]));
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public void ResolveConnectionBusyTimeout_PreservesExplicitCommandTimeoutAlias()
    {
        MethodInfo method = typeof(SQLite).GetMethod(
            "ResolveConnectionBusyTimeout",
            BindingFlags.NonPublic | BindingFlags.Static)!;

        object? result = method.Invoke(
            null,
            new object?[] { "Data Source=app.db;Command Timeout=9", null });

        Assert.Equal(0, Assert.IsType<int>(result));
    }

    [Fact]
    public void OpenSession_ReusesConnectionForAttachedDatabase()
    {
        string primary = Path.Join(Path.GetTempPath(), Path.GetFileName($"{Guid.NewGuid():N}.db"));
        string legacy = Path.Join(Path.GetTempPath(), Path.GetFileName($"{Guid.NewGuid():N}.db"));
        try
        {
            using var sqlite = new SQLite();
            sqlite.ExecuteNonQuery(legacy, "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL);");
            sqlite.ExecuteNonQuery(legacy, "INSERT INTO items (name) VALUES ($name);", new Dictionary<string, object?> { ["$name"] = "legacy" });

            using SQLiteSession session = sqlite.OpenSession(primary);
            session.ExecuteNonQuery("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL);");
            session.ExecuteNonQuery("ATTACH DATABASE $path AS legacy;", new Dictionary<string, object?> { ["$path"] = legacy });
            session.ExecuteNonQuery("INSERT INTO items (name) SELECT name FROM legacy.items;");
            session.ExecuteNonQuery("DETACH DATABASE legacy;");

            IReadOnlyList<string> rows = session.QueryAsList(
                "SELECT name FROM items ORDER BY id;",
                row => row.GetString(0));

            Assert.Equal(["legacy"], rows);
        }
        finally
        {
            Cleanup(primary);
            Cleanup(legacy);
        }
    }

    [Fact]
    public void RunInTransaction_RollsBackWhenOperationFails()
    {
        string path = Path.Join(Path.GetTempPath(), Path.GetFileName($"{Guid.NewGuid():N}.db"));
        try
        {
            using var sqlite = new SQLite();
            using SQLiteSession session = sqlite.OpenSession(path);
            session.ExecuteNonQuery("CREATE TABLE items (name TEXT NOT NULL);");

            Assert.Throws<InvalidOperationException>(() =>
                session.RunInTransaction(tx =>
                {
                    tx.ExecuteNonQuery("INSERT INTO items (name) VALUES ($name);", new Dictionary<string, object?> { ["$name"] = "temp" });
                    throw new InvalidOperationException("stop");
                }));

            object? count = session.ExecuteScalar("SELECT COUNT(*) FROM items;");
            Assert.Equal(0L, count);
        }
        finally
        {
            Cleanup(path);
        }
    }

    private static void Cleanup(string path)
    {
        TryDelete(path);
        TryDelete(path + "-wal");
        TryDelete(path + "-shm");
    }

    private static object ReadPragma(DbConnection connection, string name)
    {
        using DbCommand command = connection.CreateCommand();
        command.CommandText = $"PRAGMA {name};";
        return command.ExecuteScalar()!;
    }

    private static long ReadCount(DbConnection connection, DbTransaction? transaction)
    {
        using DbCommand command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT COUNT(*) FROM items;";
        return Convert.ToInt64(command.ExecuteScalar());
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }
}
