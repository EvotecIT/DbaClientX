using DBAClientX.QueryBuilder;
using Xunit;

namespace DbaClientX.Tests;

public class QueryBuilderUpsertEdgeTests
{
    [Fact]
    public void UpsertUpdateOnly_IgnoresKey_PostgreSql()
    {
        var q = new Query()
            .InsertOrUpdate("Users", new[] { ("Id", (object)1), ("Name", (object)"Bob"), ("Email", (object)"bob@example.com") }, "Id")
            .UpsertUpdateOnly("Id", "Name");

        var sql = QueryBuilder.Compile(q, SqlDialect.PostgreSql);
        Assert.Equal("INSERT INTO \"Users\" (\"Id\", \"Name\", \"Email\") VALUES (1, 'Bob', 'bob@example.com') ON CONFLICT (\"Id\") DO UPDATE SET \"Name\" = EXCLUDED.\"Name\"", sql);
    }

    [Fact]
    public void UpsertUpdateOnly_IgnoresKey_SqlServer_WithSchema()
    {
        var q = new Query()
            .InsertOrUpdate("dbo.Users", new[] { ("Id", (object)1), ("Name", (object)"Bob"), ("Email", (object)"bob@example.com") }, "Id")
            .UpsertUpdateOnly("Id", "Name");

        var sql = QueryBuilder.Compile(q, SqlDialect.SqlServer);
        var expected = "MERGE INTO [dbo].[Users] AS target USING (VALUES (1, 'Bob', 'bob@example.com')) AS source ([Id], [Name], [Email]) ON (target.[Id] = source.[Id]) WHEN MATCHED THEN UPDATE SET target.[Name] = source.[Name] WHEN NOT MATCHED THEN INSERT ([Id], [Name], [Email]) VALUES (source.[Id], source.[Name], source.[Email])";
        Assert.Equal(expected, sql);
    }

    [Fact]
    public void UpsertUpdateOnly_ChangesSql_AffectsCacheKey()
    {
        var q1 = new Query()
            .InsertOrUpdate("users", new[] { ("id", (object)1), ("name", (object)"Bob") }, "id")
            .UpsertUpdateOnly("name");
        var q2 = new Query()
            .InsertOrUpdate("users", new[] { ("id", (object)1), ("name", (object)"Bob") }, "id")
            .UpsertUpdateOnly("id"); // key will be ignored => no update columns

        var sql1 = QueryBuilder.Compile(q1, SqlDialect.PostgreSql);
        var sql2 = QueryBuilder.Compile(q2, SqlDialect.PostgreSql);
        Assert.NotEqual(sql1, sql2);
        Assert.EndsWith("DO UPDATE SET \"name\" = EXCLUDED.\"name\"", sql1);
        Assert.DoesNotContain("DO UPDATE SET", sql2);
    }
}
