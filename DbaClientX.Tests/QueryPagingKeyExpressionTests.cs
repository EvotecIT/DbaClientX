using System.Data;
using DBAClientX;
using DBAClientX.QueryBuilder;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

public sealed class QueryPagingKeyExpressionTests
{
    [Theory]
    [InlineData(SqlDialect.SQLite, "SELECT * FROM \"hosts\" WHERE +\"seen\" <= @p0 AND ((+\"seen\" < @p1) OR (+\"seen\" = @p2 AND \"name\" COLLATE DBX_NOCASE > @p3) OR (+\"seen\" = @p4 AND \"name\" COLLATE DBX_NOCASE = @p5 AND \"id\" > @p6)) ORDER BY +\"seen\" DESC, \"name\" COLLATE DBX_NOCASE, \"id\" LIMIT 3")]
    [InlineData(SqlDialect.PostgreSql, "SELECT * FROM \"hosts\" WHERE +\"seen\" <= @p0 AND ((+\"seen\" < @p1) OR (+\"seen\" = @p2 AND \"name\" COLLATE \"DBX_NOCASE\" > @p3) OR (+\"seen\" = @p4 AND \"name\" COLLATE \"DBX_NOCASE\" = @p5 AND \"id\" > @p6)) ORDER BY +\"seen\" DESC, \"name\" COLLATE \"DBX_NOCASE\", \"id\" LIMIT 3")]
    [InlineData(SqlDialect.SqlServer, "SELECT TOP 3 * FROM [hosts] WHERE +\"seen\" <= @p0 AND ((+\"seen\" < @p1) OR (+\"seen\" = @p2 AND [name] COLLATE DBX_NOCASE > @p3) OR (+\"seen\" = @p4 AND [name] COLLATE DBX_NOCASE = @p5 AND [id] > @p6)) ORDER BY +\"seen\" DESC, [name] COLLATE DBX_NOCASE, [id]")]
    public void CreatePageQuery_ExpressionAndCollatedKeys_CompareAndSortTheSameWay(SqlDialect dialect, string expected)
    {
        var paging = new KeysetPagination(
            2,
            new[] { KeysetColumn.Expression("+\"seen\"", "seen", descending: true), KeysetColumn.Asc("name").WithCollation("DBX_NOCASE") },
            KeysetColumn.Asc("id"));
        var cursor = paging.CreateCursor(new object?[] { 20L, "b", 7L });

        var (sql, parameters) = paging.CreatePageQuery(new Query().From("hosts"), cursor).CompileWithParameters(dialect);

        Assert.Equal(expected, sql);
        Assert.Equal(new object[] { 20L, 20L, 20L, "b", 20L, "b", 7L }, parameters);
        Assert.Equal(new[] { "+\"seen\" DESC", "name COLLATE DBX_NOCASE", "id" }, paging.CreatePageQuery(new Query().From("hosts")).OrderByColumns);
    }

    [Fact]
    public void Keys_ThatCanTieDifferentRowsOrUnsafeCollations_AreRefused()
    {
        // The last key decides between rows; an expression or collation can make different rows equal.
        Assert.Throws<ArgumentException>(() => new KeysetPagination(10, KeysetColumn.Asc("id"), KeysetColumn.Asc("name").WithCollation("NOCASE")));
        Assert.Throws<ArgumentException>(() => new KeysetPagination(10, KeysetColumn.Expression("lower(name)", "name")));
        Assert.Throws<ArgumentException>(() => new KeysetPagination(10, Array.Empty<KeysetColumn>(), KeysetColumn.Asc("id").WithCollation("BINARY")));
        Assert.Equal("uniqueKey", Assert.Throws<ArgumentException>(() => new KeysetPagination(10, Array.Empty<KeysetColumn>(), KeysetColumn.Expression("id", "id"))).ParamName);
        Assert.Throws<ArgumentNullException>(() => new KeysetPagination(10, Array.Empty<KeysetColumn>(), null!));
        Assert.Throws<ArgumentException>(() => new KeysetPagination(10, new KeysetColumn[] { null! }, KeysetColumn.Asc("id")));

        foreach (var name in new[] { "NOCASE; DROP TABLE t", "a b", "1x", "\"x", "x'", "x--", "", "ü" })
        {
            Assert.Throws<ArgumentException>(() => KeysetColumn.Asc("name").WithCollation(name));
        }

        Assert.Throws<InvalidOperationException>(() => KeysetColumn.Expression("+seen", "seen").WithCollation("NOCASE"));
        Assert.Throws<ArgumentException>(() => KeysetColumn.Expression("+seen", " "));
        Assert.Throws<ArgumentException>(() => KeysetColumn.Expression(" ", "seen"));
    }

    [Fact]
    public void Cursor_OfAnotherCollationOrExpression_IsRefused_AndPlainKeysKeepTheirCursors()
    {
        var plain = new KeysetPagination(10, KeysetColumn.Asc("name"), KeysetColumn.Asc("id"));
        var collated = new KeysetPagination(10, KeysetColumn.Asc("name").WithCollation("NOCASE"), KeysetColumn.Asc("id"));
        var otherCollation = new KeysetPagination(10, KeysetColumn.Asc("name").WithCollation("BINARY"), KeysetColumn.Asc("id"));
        var expression = new KeysetPagination(10, new[] { KeysetColumn.Expression("name", "name") }, KeysetColumn.Asc("id"));
        var keys = new object?[] { "a", 1L };

        Assert.Throws<ArgumentException>(() => collated.CreatePageQuery(new Query().From("t"), plain.CreateCursor(keys)));
        Assert.Throws<ArgumentException>(() => otherCollation.CreatePageQuery(new Query().From("t"), collated.CreateCursor(keys)));
        Assert.Throws<ArgumentException>(() => expression.CreatePageQuery(new Query().From("t"), plain.CreateCursor(keys)));
        // Plain keys keep the cursor binding of earlier versions: this cursor was issued before expressions and collations existed.
        var earlier = new KeysetPagination(10, new[] { KeysetColumn.Asc("name") }, KeysetColumn.Desc<long>("id"));
        Assert.Equal("dbax-page-v1.Aa59lE-ishZfvNHN6EOF7MUBAWECAQAAAAAAAAA", earlier.CreateCursor(new object?[] { "a", 1L }));
        Assert.NotNull(earlier.CreatePageQuery(new Query().From("t"), "dbax-page-v1.Aa59lE-ishZfvNHN6EOF7MUBAWECAQAAAAAAAAA"));
    }

    [Fact]
    public void CollatedPageQuery_SharesNoCachedSqlWithThePlainOne_AndRefusesLiteralCompile()
    {
        var plain = new KeysetPagination(10, KeysetColumn.Asc("name"), KeysetColumn.Asc("id"));
        var collated = new KeysetPagination(10, KeysetColumn.Asc("name").WithCollation("NOCASE"), KeysetColumn.Asc("id"));
        var keys = new object?[] { "a", 1L };

        var plainSql = plain.CreatePageQuery(new Query().From("t"), plain.CreateCursor(keys)).CompileWithParameters(SqlDialect.SQLite).Sql;
        var collatedSql = collated.CreatePageQuery(new Query().From("t"), collated.CreateCursor(keys)).CompileWithParameters(SqlDialect.SQLite).Sql;

        Assert.DoesNotContain("COLLATE", plainSql);
        Assert.Equal(4, collatedSql.Split("COLLATE NOCASE").Length - 1);
        Assert.Throws<InvalidOperationException>(() => collated.CreatePageQuery(new Query().From("t")).Compile(SqlDialect.SQLite));
    }

    [Fact]
    public async Task SQLite_CollatedAndExpressionKeys_ReadEveryRowOnceInTheirOrder()
    {
        var names = new[] { "b", "B", "a", "Ż", "ż", "z", "A", "ą", "Ą", "b" };
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        SQLiteUnicodeText.Register(connection);
        Execute(connection, "CREATE TABLE hosts (id INTEGER PRIMARY KEY, name TEXT NOT NULL, seen INTEGER NOT NULL); CREATE INDEX hosts_seen ON hosts (seen)");
        for (var i = 0; i < names.Length; i++)
        {
            Execute(connection, $"INSERT INTO hosts VALUES ({i + 1}, '{names[i]}', {i % 3})");
        }

        var byName = await ReadAllAsync(connection, new KeysetPagination(3, new[] { KeysetColumn.Asc("name").WithCollation(SQLiteUnicodeText.NoCaseCollation) }, KeysetColumn.Asc<long>("id")));
        var bySeen = await ReadAllAsync(connection, new KeysetPagination(3, new[] { KeysetColumn.Expression("+\"seen\"", "seen", descending: true, valueType: typeof(long)) }, KeysetColumn.Asc<long>("id")));

        var expectedByName = Enumerable.Range(1, names.Length)
            .OrderBy(id => names[id - 1], Comparer<string>.Create((a, b) => string.CompareOrdinal(a.ToLowerInvariant(), b.ToLowerInvariant())))
            .ThenBy(id => id)
            .Select(id => (long)id);
        var expectedBySeen = Enumerable.Range(1, names.Length).OrderByDescending(id => (id - 1) % 3).ThenBy(id => id).Select(id => (long)id);
        Assert.Equal(expectedByName, byName);
        Assert.Equal(expectedBySeen, bySeen);
    }

    private static async Task<List<long>> ReadAllAsync(SqliteConnection connection, KeysetPagination paging)
    {
        var ids = new List<long>();
        string? cursor = null;
        do
        {
            var (sql, parameters) = paging.CreatePageQuery(new Query().Select("id", "name", "seen").From("hosts"), cursor).CompileWithNamedParameters(SqlDialect.SQLite);
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            foreach (var parameter in parameters)
            {
                command.Parameters.AddWithValue(parameter.Key, parameter.Value);
            }

            var table = new DataTable();
            using (var reader = await command.ExecuteReaderAsync())
            {
                table.Load(reader);
            }

            var page = paging.CreatePage(table);
            ids.AddRange(page.Items.Select(row => (long)row["id"]));
            cursor = page.NextCursor;
        } while (cursor != null);

        return ids;
    }

    private static void Execute(SqliteConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }
}
