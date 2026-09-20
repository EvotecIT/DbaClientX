using DBAClientX.QueryBuilder;

namespace DbaClientX.Tests;

[CollectionDefinition(nameof(QueryCompilerCacheCollection), DisableParallelization = true)]
public sealed class QueryCompilerCacheCollection;

[Collection(nameof(QueryCompilerCacheCollection))]
public class QueryCompilerCacheTests
{
    [Fact]
    public void CompileUsesCache()
    {
        QueryCompiler.ClearCache();
        var compiler = new QueryCompiler(SqlDialect.SqlServer);
        var query = new Query().Select("*").From("users").Where("id", 1);

        var (sql1, parameters1) = compiler.CompileWithParameters(query);
        var countAfterFirst = QueryCompiler.CacheCount;

        var (sql2, parameters2) = compiler.CompileWithParameters(query);
        var countAfterSecond = QueryCompiler.CacheCount;

        Assert.Equal(sql1, sql2);
        Assert.Equal(parameters1, parameters2);
        Assert.Equal(countAfterFirst, countAfterSecond);
    }

    [Fact]
    public void CacheHit_CollectsCurrentParameterValuesInCompilerOrder()
    {
        QueryCompiler.ClearCache();
        var compiler = new QueryCompiler(SqlDialect.PostgreSql);
        var first = new Query()
            .Select("id")
            .From(new Query().Select("id").From("accounts").Where("tenant", 1), "a")
            .Where("active", true)
            .HavingRaw("COUNT(*)", ">", 3)
            .Union(new Query().Select("id").From("archived").Where("tenant", 1));
        var second = new Query()
            .Select("id")
            .From(new Query().Select("id").From("accounts").Where("tenant", 9), "a")
            .Where("active", false)
            .HavingRaw("COUNT(*)", ">", 7)
            .Union(new Query().Select("id").From("archived").Where("tenant", 9));

        var (firstSql, firstParameters) = compiler.CompileWithParameters(first);
        var (secondSql, secondParameters) = compiler.CompileWithParameters(second);

        Assert.Equal(firstSql, secondSql);
        Assert.Equal(new object[] { 1, true, 3, 1 }, firstParameters);
        Assert.Equal(new object[] { 9, false, 7, 9 }, secondParameters);
    }

    [Fact]
    public void SqlServerUpsertCacheHit_CollectsFreshInsertConflictAndUpdateValues()
    {
        QueryCompiler.ClearCache();
        var compiler = new QueryCompiler(SqlDialect.SqlServer);
        var first = new Query().InsertOrUpdate(
            "users",
            new[] { ("id", (object)1), ("email", "first@example.test"), ("name", "First") },
            "id",
            "email");
        var second = new Query().InsertOrUpdate(
            "users",
            new[] { ("id", (object)2), ("email", "second@example.test"), ("name", "Second") },
            "id",
            "email");

        var (firstSql, firstParameters) = compiler.CompileWithParameters(first);
        var (secondSql, secondParameters) = compiler.CompileWithParameters(second);

        Assert.Equal(firstSql, secondSql);
        Assert.Equal(new object[] { 1, "first@example.test", "First", 1, "first@example.test", "First" }, firstParameters);
        Assert.Equal(new object[] { 2, "second@example.test", "Second", 2, "second@example.test", "Second" }, secondParameters);
    }

    [Fact]
    public void EmptyInsertShape_CacheHitMatchesInitialCompilation()
    {
        QueryCompiler.ClearCache();
        var compiler = new QueryCompiler(SqlDialect.PostgreSql);
        var query = new Query().InsertInto("audit_events", "id", "payload");

        var first = compiler.CompileWithParameters(query);
        var second = compiler.CompileWithParameters(query);

        Assert.Equal(first.Sql, second.Sql);
        Assert.Empty(first.Parameters);
        Assert.Empty(second.Parameters);
    }

    [Fact]
    public void CacheIsLimited()
    {
        QueryCompiler.ClearCache();
        var compiler = new QueryCompiler(SqlDialect.SqlServer);
        for (int i = 0; i < QueryCompiler.CacheSizeLimit + 10; i++)
        {
            var q = new Query().Select("*").From($"t{i}");
            compiler.CompileWithParameters(q);
        }
        Assert.True(QueryCompiler.CacheCount <= QueryCompiler.CacheSizeLimit);
    }

    [Fact]
    public void Compile_DoesNotReuseLiteralSqlAcrossDifferentValues()
    {
        QueryCompiler.ClearCache();
        var compiler = new QueryCompiler(SqlDialect.SqlServer);

        var sql1 = compiler.Compile(new Query().Select("*").From("users").Where("id", 1));
        var sql2 = compiler.Compile(new Query().Select("*").From("users").Where("id", 2));

        Assert.NotEqual(sql1, sql2);
        Assert.Equal("SELECT * FROM [users] WHERE [id] = 1", sql1);
        Assert.Equal("SELECT * FROM [users] WHERE [id] = 2", sql2);
        Assert.Equal(0, QueryCompiler.CacheCount);
    }

    [Fact]
    public void CompileWithParameters_DistinguishesRawJoinConditions()
    {
        QueryCompiler.ClearCache();
        var compiler = new QueryCompiler(SqlDialect.PostgreSql);
        var q1 = new Query().Select("u.id").From("users", "u").JoinRaw("orders o", "u.id = o.user_id");
        var q2 = new Query().Select("u.id").From("users", "u").JoinRaw("orders o", "u.email = o.email");

        var (sql1, _) = compiler.CompileWithParameters(q1);
        var (sql2, _) = compiler.CompileWithParameters(q2);

        Assert.NotEqual(sql1, sql2);
    }

    [Fact]
    public void CompileWithParameters_DistinguishesDelimiterBearingIdentifiers()
    {
        QueryCompiler.ClearCache();
        var compiler = new QueryCompiler(SqlDialect.PostgreSql);
        var first = new Query().Select("*").From("a:b", "c");
        var second = new Query().Select("*").From("a", "b:c");

        var (firstSql, _) = compiler.CompileWithParameters(first);
        var (secondSql, _) = compiler.CompileWithParameters(second);

        Assert.Equal("SELECT * FROM \"a:b\" AS \"c\"", firstSql);
        Assert.Equal("SELECT * FROM \"a\" AS \"b:c\"", secondSql);
        Assert.NotEqual(firstSql, secondSql);
    }

    [Fact]
    public void CompileWithParameters_DistinguishesRawAndIdentifierExpressions()
    {
        QueryCompiler.ClearCache();
        var compiler = new QueryCompiler(SqlDialect.PostgreSql);

        var (identifierSql, _) = compiler.CompileWithParameters(new Query().Select("COUNT(*)").From("users"));
        var (rawSql, _) = compiler.CompileWithParameters(new Query().SelectRaw("COUNT(*)").From("users"));

        Assert.Equal("SELECT \"COUNT(*)\" FROM \"users\"", identifierSql);
        Assert.Equal("SELECT COUNT(*) FROM \"users\"", rawSql);
    }

    [Fact]
    public void CompileWithParameters_DistinguishesRawPredicateSubqueries()
    {
        QueryCompiler.ClearCache();
        var compiler = new QueryCompiler(SqlDialect.PostgreSql);
        var activeUsers = new Query().Select("user_id").From("active_users").Where("enabled", true);
        var archivedUsers = new Query().Select("user_id").From("archived_users").Where("enabled", true);
        var first = new Query().Select("*").From("users").WhereRaw("LOWER(name)", "IN", activeUsers);
        var second = new Query().Select("*").From("users").WhereRaw("LOWER(name)", "IN", archivedUsers);

        var (firstSql, firstParameters) = compiler.CompileWithParameters(first);
        var (secondSql, secondParameters) = compiler.CompileWithParameters(second);

        Assert.Contains("FROM \"active_users\"", firstSql);
        Assert.Contains("FROM \"archived_users\"", secondSql);
        Assert.NotEqual(firstSql, secondSql);
        Assert.Equal(new object[] { true }, firstParameters);
        Assert.Equal(new object[] { true }, secondParameters);
    }
}
