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
    public void SelectOrderByDescending()
    {
        var query = new Query()
            .Select("*")
            .From("users")
            .OrderByDescending("age");

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT * FROM users ORDER BY age DESC", sql);
    }

    [Fact]
    public void SelectOrderByRaw()
    {
        var query = new Query()
            .Select("*")
            .From("users")
            .OrderByRaw("RAND()");

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT * FROM users ORDER BY RAND()", sql);
    }

    [Fact]
    public void LimitThenTop_UsesTop()
    {
        var query = new Query()
            .Select("*")
            .From("users")
            .Limit(5)
            .Offset(2)
            .Top(3);

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT TOP 3 * FROM users", sql);
    }

    [Fact]
    public void TopThenLimit_UsesLimit()
    {
        var query = new Query()
            .Top(5)
            .Limit(2)
            .Select("*")
            .From("users");

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT * FROM users LIMIT 2", sql);
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
    public void WhereNullCondition()
    {
        var query = new Query()
            .Select("*")
            .From("users")
            .WhereNull("deleted_at");

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT * FROM users WHERE deleted_at IS NULL", sql);
    }

    [Fact]
    public void WhereNotNullCondition()
    {
        var query = new Query()
            .Select("*")
            .From("users")
            .WhereNotNull("email");

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT * FROM users WHERE email IS NOT NULL", sql);
    }

    [Fact]
    public void OrWhereNullCondition()
    {
        var query = new Query()
            .Select("*")
            .From("users")
            .Where("age", ">", 18)
            .OrWhereNull("deleted_at");

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT * FROM users WHERE age > 18 OR deleted_at IS NULL", sql);
    }

    [Fact]
    public void WhereInCondition()
    {
        var query = new Query()
            .Select("*")
            .From("users")
            .WhereIn("id", 1, 2, 3);

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT * FROM users WHERE id IN (1, 2, 3)", sql);
    }

    [Fact]
    public void OrWhereInCondition()
    {
        var query = new Query()
            .Select("*")
            .From("users")
            .Where("age", ">", 18)
            .OrWhereIn("id", 1, 2);

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT * FROM users WHERE age > 18 OR id IN (1, 2)", sql);
    }

    [Fact]
    public void WhereNotInCondition()
    {
        var query = new Query()
            .Select("*")
            .From("users")
            .WhereNotIn("id", 1, 2);

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT * FROM users WHERE id NOT IN (1, 2)", sql);
    }

    [Fact]
    public void WhereBetweenCondition()
    {
        var query = new Query()
            .Select("*")
            .From("users")
            .WhereBetween("age", 18, 30);

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT * FROM users WHERE age BETWEEN 18 AND 30", sql);
    }

    [Fact]
    public void OrWhereBetweenCondition()
    {
        var query = new Query()
            .Select("*")
            .From("users")
            .Where("status", "=", "active")
            .OrWhereBetween("age", 18, 30);

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT * FROM users WHERE status = 'active' OR age BETWEEN 18 AND 30", sql);
    }

    [Fact]
    public void WhereNotBetweenCondition()
    {
        var query = new Query()
            .Select("*")
            .From("users")
            .WhereNotBetween("age", 18, 30);

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT * FROM users WHERE age NOT BETWEEN 18 AND 30", sql);
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
    public void NestedSubqueryInWhere()
    {
        var inner = new Query()
            .Select("id")
            .From("admins");

        var middle = new Query()
            .Select("id")
            .From("users")
            .Where("owner_id", "IN", inner);

        var query = new Query()
            .Select("*")
            .From("items")
            .Where("user_id", "IN", middle);

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT * FROM items WHERE user_id IN (SELECT id FROM users WHERE owner_id IN (SELECT id FROM admins))", sql);
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
    public void OrConditionsWithGrouping()
    {
        var query = new Query()
            .Select("*")
            .From("users")
            .BeginGroup()
                .Where("age", "<", 18)
                .OrWhere("age", ">", 60)
            .EndGroup()
            .Where("active", true);

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT * FROM users WHERE (age < 18 OR age > 60) AND active = 1", sql);
    }

    [Fact]
    public void EndGroupWithoutBegin_Throws()
    {
        var query = new Query()
            .Select("*")
            .From("users");

        Assert.Throws<InvalidOperationException>(() => query.EndGroup());
    }

    [Fact]
    public void UnclosedGroup_ThrowsOnCompile()
    {
        var query = new Query()
            .Select("*")
            .From("users")
            .BeginGroup()
            .Where("age", "<", 18);

        Assert.Throws<InvalidOperationException>(() => QueryBuilder.Compile(query));
    }

    [Fact]
    public void JoinQueries()
    {
        var query = new Query()
            .Select("u.name", "o.total")
            .From("users u")
            .Join("orders o", "u.id = o.user_id");

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id", sql);
    }

    [Fact]
    public void MultipleJoinTypes()
    {
        var query = new Query()
            .Select("*")
            .From("users u")
            .LeftJoin("profiles p", "u.id = p.user_id")
            .RightJoin("photos ph", "u.id = ph.user_id");

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT * FROM users u LEFT JOIN profiles p ON u.id = p.user_id RIGHT JOIN photos ph ON u.id = ph.user_id", sql);
    }
    [Fact]
    public void CrossJoinQueries()
    {
        var query = new Query()
            .Select("*")
            .From("users u")
            .CrossJoin("orders o");

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT * FROM users u CROSS JOIN orders o", sql);
    }

    [Fact]
    public void FullOuterJoinQueries()
    {
        var query = new Query()
            .Select("u.name", "o.total")
            .From("users u")
            .FullOuterJoin("orders o", "u.id = o.user_id");

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT u.name, o.total FROM users u FULL OUTER JOIN orders o ON u.id = o.user_id", sql);
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

    [Fact]
    public void CompileWithParameters_ReturnsSqlAndParameters()
    {
        var query = new Query()
            .Select("*")
            .From("users")
            .Where("id", 1)
            .Where("name", "Bob");

        var (sql, parameters) = QueryBuilder.CompileWithParameters(query);
        Assert.Equal("SELECT * FROM users WHERE id = @p0 AND name = @p1", sql);
        Assert.Equal(new object[] { 1, "Bob" }, parameters);
    }

    [Fact]
    public void UnionQueries()
    {
        var query = new Query()
            .Select("id")
            .From("users1")
            .Union(new Query().Select("id").From("users2"));

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT id FROM users1 UNION SELECT id FROM users2", sql);
    }

    [Fact]
    public void UnionAllQueries()
    {
        var query = new Query()
            .Select("id")
            .From("users1")
            .UnionAll(new Query().Select("id").From("users2"));

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT id FROM users1 UNION ALL SELECT id FROM users2", sql);
    }

    [Fact]
    public void IntersectQueries()
    {
        var query = new Query()
            .Select("id")
            .From("users1")
            .Intersect(new Query().Select("id").From("users2"));

        var sql = QueryBuilder.Compile(query);
        Assert.Equal("SELECT id FROM users1 INTERSECT SELECT id FROM users2", sql);
    }

    [Fact]
    public void Select_WithNoColumns_Throws()
    {
        var query = new Query();
        Assert.Throws<ArgumentException>(() => query.Select());
    }

    [Fact]
    public void From_WithNullTable_Throws()
    {
        var query = new Query();
        Assert.Throws<ArgumentException>(() => query.From(null!));
    }

    [Fact]
    public void InsertInto_WithoutColumns_Throws()
    {
        var query = new Query();
        Assert.Throws<ArgumentException>(() => query.InsertInto("users"));
    }

    [Fact]
    public void CrossJoin_WithEmptyTable_Throws()
    {
        var query = new Query();
        Assert.Throws<ArgumentException>(() => query.CrossJoin(""));
    }

    [Fact]
    public void FullOuterJoin_WithNullCondition_Throws()
    {
        var query = new Query();
        Assert.Throws<ArgumentException>(() => query.FullOuterJoin("orders", null!));
    }

    [Fact]
    public void Join_WithNullCondition_Throws()
    {
        var query = new Query();
        Assert.Throws<ArgumentException>(() => query.Join("users", null!));
    }

    [Fact]
    public void Where_WithEmptyColumn_Throws()
    {
        var query = new Query();
        Assert.Throws<ArgumentException>(() => query.Where("", 1));
    }
}

