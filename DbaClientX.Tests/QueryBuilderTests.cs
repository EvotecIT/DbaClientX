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
}

