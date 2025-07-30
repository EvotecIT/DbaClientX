using DBAClientX.QueryBuilder;
using System.Globalization;

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
    public void SelectLimitOffset()
    {
        var query = new Query()
            .Select("*")
            .From("users")
            .OrderBy("name")
            .Limit(5)
            .Offset(2);

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT * FROM users ORDER BY name LIMIT 5 OFFSET 2", sql);
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

    [Fact]
    public void SelectWhereOrderByTop()
    {
        var query = new Query()
            .Top(3)
            .Select("name")
            .From("users")
            .Where("age", ">", 18)
            .OrderBy("age");

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT TOP 3 name FROM users WHERE age > 18 ORDER BY age", sql);
    }

    [Fact]
    public void SelectMultipleColumns()
    {
        var query = new Query()
            .Select("name", "age")
            .From("users");

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT name, age FROM users", sql);
    }

    [Fact]
    public void MultipleWhereClauses()
    {
        var query = new Query()
            .Select("*")
            .From("users")
            .Where("age", ">", 18)
            .Where("active", true);

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT * FROM users WHERE age > 18 AND active = 1", sql);
    }

    [Fact]
    public void SelectWithoutFrom()
    {
        var query = new Query()
            .Select("1");

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT 1", sql);
    }

    [Fact]
    public void SubqueryInFrom()
    {
        var sub = new Query()
            .Select("*")
            .From("users");

        var query = new Query()
            .Select("u.id")
            .From(sub, "u");

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT u.id FROM (SELECT * FROM users) AS u", sql);
    }

    [Fact]
    public void SubqueryInWhere()
    {
        var sub = new Query()
            .Select("id")
            .From("admins");

        var query = new Query()
            .Select("*")
            .From("users")
            .Where("id", "IN", sub);

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT * FROM users WHERE id IN (SELECT id FROM admins)", sql);
    }

    [Fact]
    public void GroupByHaving()
    {
        var query = new Query()
            .Select("age", "COUNT(*)")
            .From("users")
            .GroupBy("age")
            .Having("COUNT(*)", ">", 1);

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT age, COUNT(*) FROM users GROUP BY age HAVING COUNT(*) > 1", sql);
    }

    [Fact]
    public void DecimalFormatting_UsesInvariantCulture()
    {
        var original = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo("pl-PL");
            var query = new Query()
                .Select("*")
                .From("prices")
                .Where("amount", 10.5m);

            var sql = QueryBuilder.Compile(query);
            Assert.Equal("SELECT * FROM prices WHERE amount = 10.5", sql);
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }

    [Fact]
    public void DateTimeFormatting_UsesInvariantCulture()
    {
        var original = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo("pl-PL");
            var date = new DateTime(2024, 1, 2, 3, 4, 5);
            var query = new Query()
                .Select("*")
                .From("events")
                .Where("created", date);

            var sql = QueryBuilder.Compile(query);
            Assert.Equal("SELECT * FROM events WHERE created = '2024-01-02 03:04:05'", sql);
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }

    [Fact]
    public void DateTimeOffsetFormatting_UsesInvariantCulture()
    {
        var original = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo("pl-PL");
            var dateOffset = new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.FromHours(2));
            var query = new Query()
                .Select("*")
                .From("events")
                .Where("created", dateOffset);

            var sql = QueryBuilder.Compile(query);
            Assert.Equal("SELECT * FROM events WHERE created = '2024-01-02 03:04:05'", sql);
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }
}

