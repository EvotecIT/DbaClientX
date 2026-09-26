using System.Data;
using DBAClientX.QueryBuilder;

namespace DbaClientX.Tests;

public class QueryPagingTests
{
    private static readonly KeysetColumn[] CreatedThenId =
    {
        KeysetColumn.Desc("created"),
        KeysetColumn.Asc("id"),
    };

    [Theory]
    [InlineData(SqlDialect.SqlServer, "SELECT TOP 3 * FROM [events] WHERE [zone] = @p0 ORDER BY [created] DESC, [id]")]
    [InlineData(SqlDialect.PostgreSql, "SELECT * FROM \"events\" WHERE \"zone\" = @p0 ORDER BY \"created\" DESC, \"id\" LIMIT 3")]
    [InlineData(SqlDialect.MySql, "SELECT * FROM `events` WHERE `zone` = @p0 ORDER BY `created` DESC, `id` LIMIT 3")]
    [InlineData(SqlDialect.SQLite, "SELECT * FROM \"events\" WHERE \"zone\" = @p0 ORDER BY \"created\" DESC, \"id\" LIMIT 3")]
    [InlineData(SqlDialect.Oracle, "SELECT * FROM \"events\" WHERE \"zone\" = :p0 ORDER BY \"created\" DESC, \"id\" FETCH FIRST 3 ROWS ONLY")]
    public void KeysetCreatePageQuery_FirstPage_OrdersByKeysAndFetchesOneExtraRow(SqlDialect dialect, string expected)
    {
        var paging = new KeysetPagination(2, CreatedThenId);

        var (sql, parameters) = paging.CreatePageQuery(new Query().From("events").Where("zone", "eu")).CompileWithParameters(dialect);

        Assert.Equal(expected, sql);
        Assert.Equal(new object[] { "eu" }, parameters);
    }

    [Theory]
    [InlineData(SqlDialect.SqlServer, "SELECT TOP 3 * FROM [events] WHERE ([zone] = @p0 OR [zone] = @p1) AND [created] <= @p2 AND (([created] < @p3) OR ([created] = @p4 AND [id] > @p5)) ORDER BY [created] DESC, [id]")]
    [InlineData(SqlDialect.Oracle, "SELECT * FROM \"events\" WHERE (\"zone\" = :p0 OR \"zone\" = :p1) AND \"created\" <= :p2 AND ((\"created\" < :p3) OR (\"created\" = :p4 AND \"id\" > :p5)) ORDER BY \"created\" DESC, \"id\" FETCH FIRST 3 ROWS ONLY")]
    [InlineData(SqlDialect.SQLite, "SELECT * FROM \"events\" WHERE (\"zone\" = @p0 OR \"zone\" = @p1) AND \"created\" <= @p2 AND ((\"created\" < @p3) OR (\"created\" = @p4 AND \"id\" > @p5)) ORDER BY \"created\" DESC, \"id\" LIMIT 3")]
    public void KeysetCreatePageQuery_WithCursor_GroupsSourceConditionsAndSeeksPastLastKey(SqlDialect dialect, string expected)
    {
        var paging = new KeysetPagination(2, CreatedThenId);
        var cursor = paging.CreateCursor(new object?[] { 20L, 7 });
        var source = new Query().From("events").Where("zone", "eu").OrWhere("zone", "us");

        var (sql, parameters) = paging.CreatePageQuery(source, cursor).CompileWithParameters(dialect);

        Assert.Equal(expected, sql);
        Assert.Equal(new object[] { "eu", "us", 20L, 20L, 20L, 7L }, parameters);
        Assert.Equal("SELECT * FROM [events] WHERE [zone] = 'eu' OR [zone] = 'us'", source.Compile());
    }

    [Fact]
    public void KeysetCreatePageQuery_SingleKey_UsesOneComparison()
    {
        var paging = new KeysetPagination(10, KeysetColumn.Asc("id"));

        var (sql, parameters) = paging.CreatePageQuery(new Query().From("users"), paging.CreateCursor(new object?[] { "k" }))
            .CompileWithParameters(SqlDialect.PostgreSql);

        Assert.Equal("SELECT * FROM \"users\" WHERE \"id\" > @p0 ORDER BY \"id\" LIMIT 11", sql);
        Assert.Equal(new object[] { "k" }, parameters);
    }

    [Theory]
    [InlineData(SqlDialect.SqlServer, "SELECT * FROM [users] ORDER BY [id] OFFSET 20 ROWS FETCH NEXT 11 ROWS ONLY")]
    [InlineData(SqlDialect.PostgreSql, "SELECT * FROM \"users\" ORDER BY \"id\" LIMIT 11 OFFSET 20")]
    [InlineData(SqlDialect.MySql, "SELECT * FROM `users` ORDER BY `id` LIMIT 11 OFFSET 20")]
    [InlineData(SqlDialect.SQLite, "SELECT * FROM \"users\" ORDER BY \"id\" LIMIT 11 OFFSET 20")]
    [InlineData(SqlDialect.Oracle, "SELECT * FROM \"users\" ORDER BY \"id\" OFFSET 20 ROWS FETCH NEXT 11 ROWS ONLY")]
    public void OffsetCreatePageQuery_PageIndex_UsesDialectOffset(SqlDialect dialect, string expected)
    {
        var paging = new OffsetPagination(10);

        var sql = paging.CreatePageQuery(new Query().From("users").OrderBy("id"), pageIndex: 2).Compile(dialect);

        Assert.Equal(expected, sql);
    }

    [Fact]
    public void CreatePageQuery_UnsupportedSource_Throws()
    {
        var keyset = new KeysetPagination(10, KeysetColumn.Asc("id"));
        var offset = new OffsetPagination(10);

        Assert.Throws<InvalidOperationException>(() => keyset.CreatePageQuery(new Query().From("t").OrderBy("id")));
        Assert.Throws<InvalidOperationException>(() => keyset.CreatePageQuery(new Query().From("t").Limit(5)));
        Assert.Throws<InvalidOperationException>(() => keyset.CreatePageQuery(new Query().From("t").Union(new Query().From("u"))));
        Assert.Throws<InvalidOperationException>(() => keyset.CreatePageQuery(new Query().DeleteFrom("t")));
        Assert.Throws<InvalidOperationException>(() => offset.CreatePageQuery(new Query().From("t")));
    }

    [Fact]
    public void KeysetCursor_TamperedOrForeign_IsRejected()
    {
        var paging = new KeysetPagination(10, CreatedThenId);
        var other = new KeysetPagination(10, KeysetColumn.Asc("created"), KeysetColumn.Asc("id"));
        var cursor = paging.CreateCursor(new object?[] { 1L, 2L });

        Assert.Throws<ArgumentException>(() => other.CreatePageQuery(new Query().From("t"), cursor));
        Assert.Throws<ArgumentException>(() => paging.CreatePageQuery(new Query().From("t"), cursor + "AA"));
        Assert.Throws<ArgumentException>(() => paging.CreatePageQuery(new Query().From("t"), "dbax-page-v1.!!"));
        Assert.Throws<ArgumentException>(() => paging.CreatePageQuery(new Query().From("t"), new OffsetPagination(10).CreatePage(new[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 }, null).NextCursor));
        Assert.Throws<InvalidOperationException>(() => paging.CreateCursor(new object?[] { null, 2L }));
        Assert.DoesNotContain(cursor, character => character is '+' or '/' or '=');
    }

    [Fact]
    public void KeysetCreatePage_ExtraRow_TrimsAndCreatesCursorFromLastItem()
    {
        var paging = new KeysetPagination(2, KeysetColumn.Asc("id"));

        var page = paging.CreatePage(new[] { 1, 2, 3 }, row => new object?[] { row });
        var last = paging.CreatePage(new[] { 4 }, row => new object?[] { row });

        Assert.Equal(new[] { 1, 2 }, page.Items);
        Assert.True(page.HasMore);
        Assert.Equal(paging.CreateCursor(new object?[] { 2 }), page.NextCursor);
        Assert.False(last.HasMore);
        Assert.Null(last.NextCursor);
    }

    [Fact]
    public async Task KeysetPagination_SQLite_ReadsEveryRowOnceInOrder()
    {
        using var sqlite = new DBAClientX.SQLite { ReturnType = DBAClientX.ReturnType.DataTable };
        var connectionString = "Data Source=paging-keyset-" + Guid.NewGuid().ToString("N") + ";Mode=Memory;Cache=Shared";
        using var keepAlive = new Microsoft.Data.Sqlite.SqliteConnection(connectionString);
        keepAlive.Open();
        await SeedAsync(sqlite, connectionString);
        var paging = new KeysetPagination(3, KeysetColumn.Desc("created"), KeysetColumn.Asc("id"));
        var source = new Query().Select("id", "created").From("events").Where("zone", "eu").OrWhere("zone", "us");

        var ids = new List<long>();
        string? cursor = null;
        var pages = 0;
        do
        {
            var (sql, parameters) = paging.CreatePageQuery(source, cursor).CompileWithParameters(SqlDialect.SQLite);
            var table = (DataTable)(await sqlite.QueryWithConnectionStringAsync(connectionString, sql, ToDictionary(parameters)))!;
            var page = paging.CreatePage(table);
            ids.AddRange(page.Items.Select(row => (long)row["id"]));
            cursor = page.NextCursor;
            pages++;
        } while (cursor != null);

        Assert.Equal(new long[] { 5, 6, 7, 3, 4, 1, 2 }, ids);
        Assert.Equal(3, pages);
    }

    [Fact]
    public async Task OffsetPagination_SQLite_ReadsEveryRowOnceInOrder()
    {
        using var sqlite = new DBAClientX.SQLite { ReturnType = DBAClientX.ReturnType.DataTable };
        var connectionString = "Data Source=paging-offset-" + Guid.NewGuid().ToString("N") + ";Mode=Memory;Cache=Shared";
        using var keepAlive = new Microsoft.Data.Sqlite.SqliteConnection(connectionString);
        keepAlive.Open();
        await SeedAsync(sqlite, connectionString);
        var paging = new OffsetPagination(4);
        var source = new Query().Select("id").From("events").Where("zone", "<>", "ap").OrderBy("id");

        var ids = new List<long>();
        string? cursor = null;
        do
        {
            var (sql, parameters) = paging.CreatePageQuery(source, cursor).CompileWithParameters(SqlDialect.SQLite);
            var table = (DataTable)(await sqlite.QueryWithConnectionStringAsync(connectionString, sql, ToDictionary(parameters)))!;
            var page = paging.CreatePage(table, cursor);
            ids.AddRange(page.Items.Select(row => (long)row["id"]));
            cursor = page.NextCursor;
        } while (cursor != null);

        Assert.Equal(new long[] { 1, 2, 3, 4, 5, 6, 7 }, ids);
    }

    [Fact]
    public void KeysetCreatePageQuery_ThreeKeysOnMySql_ExpandsEveryPrefix()
    {
        var paging = new KeysetPagination(5, KeysetColumn.Asc("a"), KeysetColumn.Desc("b"), KeysetColumn.Asc("c"));

        var (sql, parameters) = paging.CreatePageQuery(new Query().From("t"), paging.CreateCursor(new object?[] { 1, "x", 2.5d }))
            .CompileWithParameters(SqlDialect.MySql);

        Assert.Equal(
            "SELECT * FROM `t` WHERE `a` >= @p0 AND ((`a` > @p1) OR (`a` = @p2 AND `b` < @p3) OR (`a` = @p4 AND `b` = @p5 AND `c` > @p6)) ORDER BY `a`, `b` DESC, `c` LIMIT 6",
            sql);
        Assert.Equal(new object[] { 1L, 1L, 1L, "x", 1L, "x", 2.5d }, parameters);
    }

    [Fact]
    public void KeysetCreatePageQuery_ComplexSource_KeepsClausesAndLeavesSourceUnchanged()
    {
        var paging = new KeysetPagination(10, KeysetColumn.Asc("u.Id"));
        var source = new Query()
            .Distinct()
            .Select("u.Id", "u.Name")
            .From("Users", "u")
            .Join("Orders", "o", "u.Id", "=", "o.UserId")
            .GroupBy("u.Id", "u.Name")
            .Having("u.Id", ">", 0);
        var before = source.Compile();

        var sql = paging.CreatePageQuery(source, paging.CreateCursor(new object?[] { 5 })).Compile(SqlDialect.SqlServer);

        Assert.Equal(
            "SELECT DISTINCT TOP 11 [u].[Id], [u].[Name] FROM [Users] AS [u] JOIN [Orders] AS [o] ON [u].[Id] = [o].[UserId] WHERE [u].[Id] > 5 GROUP BY [u].[Id], [u].[Name] HAVING [u].[Id] > 0 ORDER BY [u].[Id]",
            sql);
        Assert.Equal(before, source.Compile());
        Assert.Equal("Id", paging.Columns[0].ResultColumn);
    }

    [Fact]
    public void KeysetCursor_RoundTrip_PreservesKeyTypes()
    {
        var paging = new KeysetPagination(1, KeysetColumn.Asc("d"), KeysetColumn.Asc("g"), KeysetColumn.Asc("m"), KeysetColumn.Asc("i"));
        var date = new DateTime(2026, 9, 26, 10, 30, 0, DateTimeKind.Utc);
        var guid = Guid.NewGuid();

        var (_, parameters) = paging.CreatePageQuery(new Query().From("t"), paging.CreateCursor(new object?[] { date, guid, 12.5m, 7 }))
            .CompileWithParameters(SqlDialect.SqlServer);

        Assert.Equal(date, parameters[0]);
        Assert.Equal(DateTimeKind.Utc, ((DateTime)parameters[0]).Kind);
        Assert.Equal(guid, parameters[3]);
        Assert.Equal(12.5m, parameters[6]);
        Assert.Equal(7L, parameters[parameters.Count - 1]);
    }

    [Fact]
    public void KeysetCreateCursor_OversizedKey_Throws()
    {
        var paging = new KeysetPagination(1, KeysetColumn.Asc("name"));

        Assert.Throws<InvalidOperationException>(() => paging.CreateCursor(new object?[] { new string('x', 20000) }));
    }

    [Fact]
    public void OffsetPagination_CursorAndBounds_AreValidated()
    {
        var paging = new OffsetPagination(10);
        var source = new Query().From("t").OrderBy("id");
        var rows = Enumerable.Range(1, 11).ToArray();
        var second = paging.CreatePage(rows, null).NextCursor;

        Assert.Equal("SELECT * FROM \"t\" ORDER BY \"id\" LIMIT 11 OFFSET 20", paging.CreatePageQuery(source, paging.CreatePage(rows, second).NextCursor).Compile(SqlDialect.SQLite));
        Assert.Throws<ArgumentException>(() => paging.CreatePageQuery(source, new KeysetPagination(10, KeysetColumn.Asc("id")).CreateCursor(new object?[] { 1 })));
        Assert.Throws<ArgumentException>(() => paging.CreatePageQuery(source, "dbax-page-v1.Av____8"));
        Assert.Throws<ArgumentOutOfRangeException>(() => paging.CreatePageQuery(source, pageIndex: -1));
        Assert.Throws<ArgumentOutOfRangeException>(() => paging.CreatePageQuery(source, pageIndex: int.MaxValue / 5));
        Assert.Throws<ArgumentOutOfRangeException>(() => new OffsetPagination(0));
        var last = new OffsetPagination(int.MaxValue - 1);
        var nearEnd = last.CreatePageQuery(source, pageIndex: 1);
        Assert.Equal(int.MaxValue - 1, nearEnd.OffsetValue);
    }

    private static async Task SeedAsync(DBAClientX.SQLite sqlite, string connectionString)
    {
        await sqlite.ExecuteNonQueryWithConnectionStringAsync(
            connectionString,
            "CREATE TABLE events (id INTEGER PRIMARY KEY, created INTEGER NOT NULL, zone TEXT NOT NULL);" +
            "INSERT INTO events VALUES (1, 10, 'eu'), (2, 10, 'us'), (3, 20, 'eu'), (4, 20, 'eu'), (5, 30, 'us'), (6, 30, 'eu'), (7, 30, 'eu'), (8, 40, 'ap');");
    }

    private static Dictionary<string, object?> ToDictionary(IReadOnlyList<object> parameters)
        => parameters.Select((value, index) => (value, index)).ToDictionary(item => "@p" + item.index, item => (object?)item.value);
}
