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
        var expected = "UPDATE [dbo].[Users] SET [Name] = 'Bob' WHERE [Id] = 1; IF @@ROWCOUNT = 0 INSERT INTO [dbo].[Users] ([Id], [Name], [Email]) VALUES (1, 'Bob', 'bob@example.com')";
        Assert.Equal(expected, sql);
    }

    [Fact]
    public void UpsertUpdateOnly_AllKeys_SqlServer_UsesIfNotExistsInsert()
    {
        var q = new Query()
            .InsertOrUpdate("Users", new[] { ("Id", (object)1), ("Name", (object)"Bob") }, "Id")
            .UpsertUpdateOnly("Id");

        var sql = QueryBuilder.Compile(q, SqlDialect.SqlServer);

        Assert.Equal("IF NOT EXISTS (SELECT 1 FROM [Users] WHERE [Id] = 1) INSERT INTO [Users] ([Id], [Name]) VALUES (1, 'Bob')", sql);
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
