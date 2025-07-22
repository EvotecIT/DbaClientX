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
}

