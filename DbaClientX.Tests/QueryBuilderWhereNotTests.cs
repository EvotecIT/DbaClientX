using DBAClientX.QueryBuilder;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

[Collection(nameof(QueryCompilerCacheCollection))]
public sealed class QueryBuilderWhereNotTests
{
    [Theory]
    [InlineData(SqlDialect.SqlServer, "SELECT * FROM [t] WHERE [a] = @p0 AND NOT ([b] LIKE @p1 OR [c] IS NULL)")]
    [InlineData(SqlDialect.PostgreSql, "SELECT * FROM \"t\" WHERE \"a\" = @p0 AND NOT (\"b\" LIKE @p1 OR \"c\" IS NULL)")]
    [InlineData(SqlDialect.MySql, "SELECT * FROM `t` WHERE `a` = @p0 AND NOT (`b` LIKE @p1 OR `c` IS NULL)")]
    [InlineData(SqlDialect.SQLite, "SELECT * FROM \"t\" WHERE \"a\" = @p0 AND NOT (\"b\" LIKE @p1 OR \"c\" IS NULL)")]
    [InlineData(SqlDialect.Oracle, "SELECT * FROM \"t\" WHERE \"a\" = :p0 AND NOT (\"b\" LIKE :p1 OR \"c\" IS NULL)")]
    public void WhereNot_WrapsTheConditionsInNotForEveryDialect(SqlDialect dialect, string expected)
    {
        QueryCompiler.ClearCache();
        var (sql, parameters) = new Query().From("t").Where("a", 1)
            .WhereNot(q => q.Where("b", "LIKE", "%x%").OrWhereNull("c"))
            .CompileWithParameters(dialect);

        Assert.Equal(expected, sql);
        Assert.Equal(new object[] { 1, "%x%" }, parameters);
    }

    [Fact]
    public void OrWhereNot_NestsAndJoinsWithOr()
    {
        var sql = new Query().From("t").Where("a", 1)
            .OrWhereNot(q => q.Where("b", 2).WhereNot(inner => inner.WhereRaw("lower(c)", "GLOB", "*x*")))
            .Compile(SqlDialect.SQLite);

        Assert.Equal("SELECT * FROM \"t\" WHERE \"a\" = 1 OR NOT (\"b\" = 2 AND NOT (lower(c) GLOB '*x*'))", sql);
    }

    [Fact]
    public void WhereNot_DoesNotShareCachedSqlWithAPlainGroup()
    {
        QueryCompiler.ClearCache();
        var compiler = new QueryCompiler(SqlDialect.PostgreSql);

        var (grouped, _) = compiler.CompileWithParameters(new Query().From("t").BeginGroup().Where("a", 1).EndGroup());
        var (negated, _) = compiler.CompileWithParameters(new Query().From("t").WhereNot(q => q.Where("a", 1)));

        Assert.Equal("SELECT * FROM \"t\" WHERE (\"a\" = @p0)", grouped);
        Assert.Equal("SELECT * FROM \"t\" WHERE NOT (\"a\" = @p0)", negated);
    }

    [Theory]
    [InlineData("NOT GLOB")]
    [InlineData("not  regexp")]
    [InlineData("NOT RLIKE")]
    public void Where_AcceptsNegatedPatternOperators(string op)
    {
        var sql = new Query().From("t").Where("a", op, "x").Compile(SqlDialect.SQLite);

        Assert.Contains("\"a\" " + string.Join(" ", op.Split(' ', StringSplitOptions.RemoveEmptyEntries)).ToUpperInvariant() + " 'x'", sql);
    }

    public static TheoryData<string, Action<Query>> InvalidGroups => new()
    {
        { "empty", _ => { } },
        { "open group", q => q.BeginGroup().Where("a", 1) },
        { "leading OR", q => q.Or().Where("a", 1) },
        { "trailing OR", q => q.Where("a", 1).Or() },
        { "select", q => q.Select("a").Where("a", 1) },
        { "from", q => q.From("x").Where("a", 1) },
        { "limit", q => q.Where("a", 1).Limit(1) },
        { "empty group", q => q.BeginGroup().EndGroup() },
        { "OR before group end", q => q.BeginGroup().Where("a", 1).Or().EndGroup() },
        { "OR after group start", q => q.BeginGroup().Or().Where("a", 1).EndGroup() },
        { "double OR", q => q.Where("a", 1).Or().Or().Where("b", 2) },
        { "upsert columns", q => q.Where("a", 1).UpsertUpdateOnly("b") },
        { "order", q => q.Where("a", 1).OrderBy("a") }
    };

    [Fact]
    public void WhereNot_GroupedWithANullTest_KeepsTheOuterConditionForEveryRow()
    {
        var (sql, parameters) = new Query().DeleteFrom("t").Where("tenant", 5)
            .BeginGroup().WhereNot(q => q.Where("status", "active")).OrWhereNull("status").EndGroup()
            .CompileWithParameters(SqlDialect.PostgreSql);

        Assert.Equal("DELETE FROM \"t\" WHERE \"tenant\" = @p0 AND (NOT (\"status\" = @p1) OR \"status\" IS NULL)", sql);
        Assert.Equal(new object[] { 5, "active" }, parameters);
    }

    [Fact]
    public void WhereNot_CacheHit_CollectsTheCurrentValuesInOrder()
    {
        QueryCompiler.ClearCache();
        var compiler = new QueryCompiler(SqlDialect.SqlServer);
        Query Build(int tenant, string name, int archived) => new Query().Update("t").Set("flag", 1).Where("tenant", tenant)
            .WhereNot(q => q.Where("name", "LIKE", name).WhereIn("id", new Query().Select("id").From("archive").Where("year", archived)));

        var first = compiler.CompileWithParameters(Build(1, "a%", 2020));
        var second = compiler.CompileWithParameters(Build(2, "b%", 2021));

        Assert.Equal(first.Sql, second.Sql);
        Assert.Equal("UPDATE [t] SET [flag] = @p0 WHERE [tenant] = @p1 AND NOT ([name] LIKE @p2 AND [id] IN (SELECT [id] FROM [archive] WHERE [year] = @p3))", second.Sql);
        Assert.Equal(new object[] { 1, 2, "b%", 2021 }, second.Parameters);
    }

    [Fact]
    public void WhereNot_InAKeysetSource_StaysInsideTheSourceGroup()
    {
        var paging = new KeysetPagination(10, KeysetColumn.Asc("Id"));
        var source = new Query().Select("Id").From("t").Where("a", 1).OrWhereNot(q => q.Where("b", 2));
        var page = paging.CreatePageQuery(source, paging.CreateCursor(new object[] { 5L }));

        var (sql, _) = page.CompileWithParameters(SqlDialect.SQLite);

        Assert.StartsWith("SELECT \"Id\" FROM \"t\" WHERE (\"a\" = @p0 OR NOT (\"b\" = @p1)) AND \"Id\" > @p2", sql);
    }

    [Theory]
    [MemberData(nameof(InvalidGroups))]
    public void WhereNot_RejectsCallbacksThatDoNotAddACompleteCondition(string name, Action<Query> conditions)
    {
        _ = name;
        Assert.Throws<ArgumentException>(() => new Query().From("t").WhereNot(conditions));
        Assert.Throws<ArgumentNullException>(() => new Query().From("t").WhereNot(null!));
    }

    [Fact]
    public async Task WhereNot_SelectsTheComplementInSqlite()
    {
        var path = Path.Combine(Path.GetTempPath(), "dbx-wherenot-" + Guid.NewGuid().ToString("N") + ".db");
        try
        {
            using var sqlite = new DBAClientX.SQLite();
            sqlite.ExecuteNonQuery(path, "CREATE TABLE t (Id INTEGER PRIMARY KEY, Name TEXT)");
            sqlite.ExecuteNonQuery(path, "INSERT INTO t VALUES (1, 'alpha'), (2, 'beta'), (3, 'ALPHA-2'), (4, NULL)");
            var (sql, parameters) = new Query().Select("Id").From("t")
                .WhereNot(q => q.Where("Name", "GLOB", "*alpha*"))
                .OrWhereNull("Name")
                .OrderBy("Id")
                .CompileWithNamedParameters(SqlDialect.SQLite);

            var ids = await sqlite.QueryReadOnlyAsListAsync(path, sql, reader => reader.GetInt64(0), parameters);

            Assert.Equal(new long[] { 2, 3, 4 }, ids);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            File.Delete(path);
        }
    }
}
