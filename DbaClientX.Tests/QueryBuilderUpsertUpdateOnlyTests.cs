using System;
using DBAClientX.QueryBuilder;
using Xunit;

namespace DbaClientX.Tests;

public class QueryBuilderUpsertUpdateOnlyTests
{
    [Fact]
    public void InsertOrUpdate_PostgreSql_WithUpsertUpdateOnly()
    {
        var query = new Query()
            .InsertOrUpdate("users", new[] { ("id", (object)1), ("name", (object)"Bob"), ("email", (object)"bob@example.com") }, "id")
            .UpsertUpdateOnly("name");

        var sql = QueryBuilder.Compile(query, SqlDialect.PostgreSql);
        Assert.Equal("INSERT INTO \"users\" (\"id\", \"name\", \"email\") VALUES (1, 'Bob', 'bob@example.com') ON CONFLICT (\"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\"", sql);
    }

    [Fact]
    public void InsertOrUpdate_MySql_WithUpsertUpdateOnly()
    {
        var query = new Query()
            .InsertOrUpdate("users", new[] { ("id", (object)1), ("name", (object)"Bob"), ("email", (object)"bob@example.com") }, "id")
            .UpsertUpdateOnly("name");

        var sql = QueryBuilder.Compile(query, SqlDialect.MySql);
        Assert.Equal("INSERT INTO `users` (`id`, `name`, `email`) VALUES (1, 'Bob', 'bob@example.com') ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)", sql);
    }

    [Fact]
    public void InsertOrUpdate_SqlServer_WithUpsertUpdateOnly()
    {
        var query = new Query()
            .InsertOrUpdate("users", new[] { ("id", (object)1), ("name", (object)"Bob"), ("email", (object)"bob@example.com") }, "id")
            .UpsertUpdateOnly("name");

        var sql = QueryBuilder.Compile(query, SqlDialect.SqlServer);
        var expected = "DECLARE @__dbaClientXTranCount int = @@TRANCOUNT; BEGIN TRY IF @__dbaClientXTranCount = 0 BEGIN TRANSACTION; ELSE SAVE TRANSACTION DbaClientXUpsert; IF EXISTS (SELECT 1 FROM [users] WITH (UPDLOCK, HOLDLOCK) WHERE [id] = 1) BEGIN UPDATE [users] SET [name] = 'Bob' WHERE [id] = 1; END ELSE BEGIN INSERT INTO [users] ([id], [name], [email]) VALUES (1, 'Bob', 'bob@example.com'); END; IF @__dbaClientXTranCount = 0 COMMIT TRANSACTION; END TRY BEGIN CATCH IF XACT_STATE() = 1 BEGIN IF @__dbaClientXTranCount = 0 ROLLBACK TRANSACTION; ELSE ROLLBACK TRANSACTION DbaClientXUpsert; END ELSE IF XACT_STATE() = -1 AND @__dbaClientXTranCount = 0 BEGIN ROLLBACK TRANSACTION; END; THROW; END CATCH";
        Assert.Equal(expected, sql);
    }
}
