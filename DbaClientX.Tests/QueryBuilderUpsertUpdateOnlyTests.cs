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
        var expected = "MERGE INTO [users] AS target USING (VALUES (1, 'Bob', 'bob@example.com')) AS source ([id], [name], [email]) ON (target.[id] = source.[id]) WHEN MATCHED THEN UPDATE SET target.[name] = source.[name] WHEN NOT MATCHED THEN INSERT ([id], [name], [email]) VALUES (source.[id], source.[name], source.[email])";
        Assert.Equal(expected, sql);
    }
}
