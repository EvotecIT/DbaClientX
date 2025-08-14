using DBAClientX.QueryBuilder;

namespace DbaClientX.Tests;

public class QueryCompilerCacheTests
{
    [Fact]
    public void CompileUsesCache()
    {
        QueryCompiler.ClearCache();
        var compiler = new QueryCompiler(SqlDialect.SqlServer);
        var query = new Query().Select("*").From("users").Where("id", 1);

        var sql1 = compiler.Compile(query);
        var countAfterFirst = QueryCompiler.CacheCount;

        var sql2 = compiler.Compile(query);
        var countAfterSecond = QueryCompiler.CacheCount;

        Assert.Equal(sql1, sql2);
        Assert.Equal(countAfterFirst, countAfterSecond);
    }

    [Fact]
    public void CacheIsLimited()
    {
        QueryCompiler.ClearCache();
        var compiler = new QueryCompiler(SqlDialect.SqlServer);
        for (int i = 0; i < QueryCompiler.CacheSizeLimit + 10; i++)
        {
            var q = new Query().Select("*").From($"t{i}");
            compiler.Compile(q);
        }
        Assert.True(QueryCompiler.CacheCount <= QueryCompiler.CacheSizeLimit);
    }
}
