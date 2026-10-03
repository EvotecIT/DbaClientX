using DBAClientX.Mapping;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

[Collection(SqlitePoolCleanupCollection.Name)]
public sealed class SQLiteReadOnlyStreamTests : IDisposable
{
    private readonly string _database = Path.Combine(Path.GetTempPath(), "dbx-ro-stream-" + Guid.NewGuid().ToString("N") + ".db");

    [Fact]
    public async Task QueryReadOnlyStreamAsync_StreamsMappedRowsWithParameters()
    {
        using var sqlite = new DBAClientX.SQLite();
        sqlite.ExecuteNonQuery(_database, "CREATE TABLE Items (Id INTEGER PRIMARY KEY, Name TEXT)");
        for (var i = 1; i <= 5; i++)
        {
            sqlite.ExecuteNonQuery(_database, "INSERT INTO Items VALUES (@id, @name)", new Dictionary<string, object?> { ["@id"] = i, ["@name"] = "n" + i });
        }

        var rows = new List<object?[]>();
        var initialized = 0;
        await foreach (var row in sqlite.QueryReadOnlyStreamAsync(
            _database,
            "SELECT Id, Name FROM Items WHERE Id > @min ORDER BY Id",
            DbaRecordMapper.Values(),
            new Dictionary<string, object?> { ["@min"] = 2 },
            initialize: _ => initialized++))
        {
            rows.Add(row);
        }

        Assert.Equal(1, initialized);
        Assert.Equal(new long[] { 3, 4, 5 }, rows.Select(r => Convert.ToInt64(r[0])));
        Assert.Equal("n5", rows[2][1]);
    }

    [Fact]
    public async Task QueryReadOnlyStreamAsync_WhenStatementWrites_FailsWithReadOnlyError()
    {
        using var sqlite = new DBAClientX.SQLite();
        sqlite.ExecuteNonQuery(_database, "CREATE TABLE Items (Id INTEGER PRIMARY KEY)");

        var exception = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await foreach (var _ in sqlite.QueryReadOnlyStreamAsync(_database, "INSERT INTO Items VALUES (1) RETURNING Id", record => record.GetInt64(0))) { }
        });

        Assert.Contains(Chain(exception), e => e is SqliteException { SqliteErrorCode: 8 });
        Assert.Equal(0L, Convert.ToInt64(sqlite.ExecuteScalar(_database, "SELECT count(*) FROM Items")));
    }

    [Fact]
    public async Task QueryReadOnlyStreamAsync_WhenFileIsMissing_DoesNotCreateIt()
    {
        using var sqlite = new DBAClientX.SQLite();

        var exception = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await foreach (var _ in sqlite.QueryReadOnlyStreamAsync(_database, "SELECT 1", record => record.GetInt64(0))) { }
        });

        Assert.Contains(Chain(exception), e => e is SqliteException { SqliteErrorCode: 14 });
        Assert.False(File.Exists(_database));
    }

    private static IEnumerable<Exception> Chain(Exception exception)
    {
        for (Exception? current = exception; current != null; current = current.InnerException)
        {
            yield return current;
        }
    }

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        try
        {
            File.Delete(_database);
        }
        catch (IOException)
        {
        }
    }
}
