using System;
using System.Collections.Generic;
using System.IO;
using DBAClientX;

namespace DbaClientX.Tests;

public class SQLiteSessionTests
{
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

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }
}
