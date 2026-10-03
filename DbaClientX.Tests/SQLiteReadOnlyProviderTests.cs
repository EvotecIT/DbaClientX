using System.Data;
using DBAClientX;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

[Collection(SqlitePoolCleanupCollection.Name)]
public sealed class SQLiteReadOnlyProviderTests : IDisposable
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), "dbaclientx-attach-" + Guid.NewGuid().ToString("N") + ".db");

    public SQLiteReadOnlyProviderTests()
    {
        using var sqlite = new SQLite();
        sqlite.ExecuteNonQuery(_path, "CREATE TABLE T (Id INTEGER); INSERT INTO T VALUES (1)");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ReadOnly_AttachedUri_DoesNotGrantWriteAccess(bool uri)
    {
        var source = uri ? new Uri(_path).AbsoluteUri + "?mode=rw" : _path;
        using var sqlite = new SQLite();
        var input = SQLite.BuildReadOnlyConnectionString(_path);
        var exception = Record.Exception(() => sqlite.ExecuteNonQueryWithConnectionString(input,
            "ATTACH DATABASE '" + source.Replace("'", "''") + "' AS alias; UPDATE alias.T SET Id = 2;"));
        Assert.NotNull(exception);
        var execution = Assert.IsType<DbaQueryExecutionException>(exception);
        Assert.Equal(uri ? 14 : 8, execution.ProviderErrorCode);
        Assert.Equal(1L, sqlite.ExecuteScalar(_path, "SELECT Id FROM T"));
    }

    [Fact]
    public void ReadOnly_AttachedDatabase_CanBeRead()
    {
        using var sqlite = new SQLite();
        var value = sqlite.ExecuteScalarWithConnectionString(SQLite.BuildReadOnlyConnectionString(_path),
            "ATTACH DATABASE '" + _path.Replace("'", "''") + "' AS alias; SELECT Id FROM alias.T");
        Assert.Equal(1L, value);
    }

    [Fact]
    public void ConflictingSources_AreRejectedByProvider()
    {
        using var sqlite = new SQLite();
        Assert.Throws<ArgumentException>(() => sqlite.ExecuteScalarWithConnectionString("Data Source=" + _path + ";FullUri=../outside.db", "SELECT 1"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReadOnly_QueryAndStream_AttachedUriCannotModifyDatabase(bool stream)
    {
        using var sqlite = new SQLite { ReturnType = ReturnType.DataTable };
        var input = SQLite.BuildReadOnlyConnectionString(_path);
        var sql = "ATTACH DATABASE '" + new Uri(_path).AbsoluteUri.Replace("'", "''") + "?mode=rw' AS alias; UPDATE alias.T SET Id = 2; SELECT Id FROM alias.T;";
        if (stream)
            await Assert.ThrowsAnyAsync<Exception>(async () => { await foreach (var row in sqlite.QueryStreamWithConnectionStringAsync(input, sql)) { } });
        else
            await Assert.ThrowsAnyAsync<Exception>(() => sqlite.QueryWithConnectionStringAsync(input, sql));
        Assert.Equal(1L, sqlite.ExecuteScalar(_path, "SELECT Id FROM T"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ReadOnly_ManagedConnection_AttachedAliasCannotModifyDatabase(bool uri)
    {
        using var sqlite = new SQLite();
        using var connection = sqlite.OpenDbConnection(_path, new SQLiteConnectionOptions { ReadOnly = true });
        using var command = connection.CreateCommand();
        var source = uri ? new Uri(_path).AbsoluteUri + "?mode=rw" : _path;
        command.CommandText = "ATTACH DATABASE '" + source.Replace("'", "''") + "' AS alias; UPDATE alias.T SET Id = 2;";
        var error = Assert.Throws<SqliteException>(() => command.ExecuteNonQuery());
        Assert.Equal(uri ? 14 : 8, error.SqliteErrorCode);
        Assert.Equal(1L, sqlite.ExecuteScalar(_path, "SELECT Id FROM T"));
    }

    [Fact]
    public void ReadOnly_UriEnabledConnection_AttachedUriCannotModifyDatabase()
    {
        using var connection = new SqliteConnection(new SqliteConnectionStringBuilder
        {
            DataSource = new Uri(_path).AbsoluteUri + "?mode=ro",
            Mode = SqliteOpenMode.ReadOnly,
            Pooling = false
        }.ToString());
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "ATTACH DATABASE '" + new Uri(_path).AbsoluteUri + "?mode=rw' AS alias; UPDATE alias.T SET Id = 2;";
        Assert.Throws<SqliteException>(() => command.ExecuteNonQuery());
        using var sqlite = new SQLite();
        Assert.Equal(1L, sqlite.ExecuteScalar(_path, "SELECT Id FROM T"));
    }

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        File.Delete(_path);
    }
}
