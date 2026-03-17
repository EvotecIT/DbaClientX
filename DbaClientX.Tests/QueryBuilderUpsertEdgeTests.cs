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
        var expected = "DECLARE @__dbaClientXTranCount int = @@TRANCOUNT; BEGIN TRY IF @__dbaClientXTranCount = 0 BEGIN TRANSACTION; ELSE SAVE TRANSACTION DbaClientXUpsert; IF EXISTS (SELECT 1 FROM [dbo].[Users] WITH (UPDLOCK, HOLDLOCK) WHERE [Id] = 1) BEGIN UPDATE [dbo].[Users] SET [Name] = 'Bob' WHERE [Id] = 1; END ELSE BEGIN INSERT INTO [dbo].[Users] ([Id], [Name], [Email]) VALUES (1, 'Bob', 'bob@example.com'); END; IF @__dbaClientXTranCount = 0 COMMIT TRANSACTION; END TRY BEGIN CATCH IF XACT_STATE() = 1 BEGIN IF @__dbaClientXTranCount = 0 ROLLBACK TRANSACTION; ELSE ROLLBACK TRANSACTION DbaClientXUpsert; END ELSE IF XACT_STATE() = -1 AND @__dbaClientXTranCount = 0 BEGIN ROLLBACK TRANSACTION; END; THROW; END CATCH";
        Assert.Equal(expected, sql);
    }

    [Fact]
    public void UpsertUpdateOnly_AllKeys_SqlServer_UsesIfNotExistsInsert()
    {
        var q = new Query()
            .InsertOrUpdate("Users", new[] { ("Id", (object)1), ("Name", (object)"Bob") }, "Id")
            .UpsertUpdateOnly("Id");

        var sql = QueryBuilder.Compile(q, SqlDialect.SqlServer);

        Assert.Equal("DECLARE @__dbaClientXTranCount int = @@TRANCOUNT; BEGIN TRY IF @__dbaClientXTranCount = 0 BEGIN TRANSACTION; ELSE SAVE TRANSACTION DbaClientXUpsert; IF NOT EXISTS (SELECT 1 FROM [Users] WITH (UPDLOCK, HOLDLOCK) WHERE [Id] = 1) BEGIN INSERT INTO [Users] ([Id], [Name]) VALUES (1, 'Bob'); END; IF @__dbaClientXTranCount = 0 COMMIT TRANSACTION; END TRY BEGIN CATCH IF XACT_STATE() = 1 BEGIN IF @__dbaClientXTranCount = 0 ROLLBACK TRANSACTION; ELSE ROLLBACK TRANSACTION DbaClientXUpsert; END ELSE IF XACT_STATE() = -1 AND @__dbaClientXTranCount = 0 BEGIN ROLLBACK TRANSACTION; END; THROW; END CATCH", sql);
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
