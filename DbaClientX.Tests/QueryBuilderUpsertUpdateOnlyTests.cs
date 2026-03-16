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
        var expected = "BEGIN TRY BEGIN TRANSACTION; IF EXISTS (SELECT 1 FROM [users] WITH (UPDLOCK, HOLDLOCK) WHERE [id] = 1) BEGIN UPDATE [users] SET [name] = 'Bob' WHERE [id] = 1; END ELSE BEGIN INSERT INTO [users] ([id], [name], [email]) VALUES (1, 'Bob', 'bob@example.com'); END; COMMIT TRANSACTION; END TRY BEGIN CATCH IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION; THROW; END CATCH";
        Assert.Equal(expected, sql);
    }
}
