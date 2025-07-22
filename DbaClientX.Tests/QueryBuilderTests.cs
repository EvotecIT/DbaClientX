using DBAClientX.QueryBuilder;

namespace DbaClientX.Tests;

public class QueryBuilderTests
{
    [Fact]
    public void SimpleSelectFromWhere()
    {
        var query = new Query()
            .Select("*")
            .From("users")
            .Where("id", 1);

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT * FROM users WHERE id = 1", sql);
    }

    [Fact]
    public void SimpleInsertIntoValues()
    {
        var query = new Query()
            .InsertInto("users", "name", "age")
            .Values("Bob", 42);

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("INSERT INTO users (name, age) VALUES ('Bob', 42)", sql);
    }

    [Fact]
    public void UpdateWithWhere()
    {
        var query = new Query()
            .Update("users")
            .Set("name", "Alice")
            .Where("id", 1);

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("UPDATE users SET name = 'Alice' WHERE id = 1", sql);
    }

    [Fact]
    public void DeleteWithWhere()
    {
        var query = new Query()
            .DeleteFrom("users")
            .Where("id", 1);

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("DELETE FROM users WHERE id = 1", sql);
    }

    [Fact]
    public void SelectOrderByLimit()
    {
        var query = new Query()
            .Select("*")
            .From("users")
            .OrderBy("name")
            .Limit(10);

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT * FROM users ORDER BY name LIMIT 10", sql);
    }

    [Fact]
    public void SelectOrderByTop()
    {
        var query = new Query()
            .Top(5)
            .Select("*")
            .From("users")
            .OrderBy("age");

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT TOP 5 * FROM users ORDER BY age", sql);
    }
}

