using DBAClientX.QueryBuilder;

namespace DbaClientX.Tests;

public sealed class QueryBuilderSafetyTests
{
    [Fact]
    public void IdentifierMethods_DoNotInferRawSqlFromPunctuation()
    {
        var sql = new Query()
            .Select("name); DROP TABLE users;--")
            .From("users u")
            .OrderBy("RANDOM()")
            .Compile(SqlDialect.PostgreSql);

        Assert.Equal(
            "SELECT \"name); DROP TABLE users;--\" FROM \"users u\" ORDER BY \"RANDOM()\"",
            sql);
    }

    [Fact]
    public void RawMethods_EmitOnlyExplicitExpressions()
    {
        var sql = new Query()
            .Select("age")
            .SelectRaw("COUNT(*)")
            .From("users")
            .GroupBy("age")
            .HavingRaw("COUNT(*)", ">", 1)
            .OrderByRaw("COUNT(*) DESC")
            .Compile(SqlDialect.PostgreSql);

        Assert.Equal(
            "SELECT \"age\", COUNT(*) FROM \"users\" GROUP BY \"age\" HAVING COUNT(*) > 1 ORDER BY COUNT(*) DESC",
            sql);
    }

    [Fact]
    public void RawWhereVariants_PreserveParameterizedValues()
    {
        var query = new Query()
            .Select("*")
            .From("users")
            .WhereInRaw("LOWER(name)", "alice", "bob")
            .OrWhereNullRaw("TRIM(email)");

        var (sql, parameters) = query.CompileWithParameters(SqlDialect.PostgreSql);

        Assert.Equal("SELECT * FROM \"users\" WHERE LOWER(name) IN (@p0, @p1) OR TRIM(email) IS NULL", sql);
        Assert.Equal(new object[] { "alice", "bob" }, parameters);
    }

    [Theory]
    [InlineData("= 1; DROP TABLE users;--")]
    [InlineData("LIKE/**/OR")]
    [InlineData("IN (SELECT 1)")]
    public void SafePredicates_RejectUnknownOperators(string value)
    {
        var query = new Query();

        var exception = Assert.Throws<ArgumentException>(() => query.Where("id", value, 1));

        Assert.Equal("op", exception.ParamName);
    }

    [Fact]
    public void InOperator_RequiresSubqueryOnGeneralPredicateOverload()
    {
        var query = new Query();

        var exception = Assert.Throws<ArgumentException>(() => query.Where("id", "IN", 1));

        Assert.Equal("value", exception.ParamName);
    }

    [Fact]
    public void SafeJoin_QuotesEveryIdentifier()
    {
        var sql = new Query()
            .Select("u.id", "o.user_id")
            .From("users", "u")
            .Join("orders", "o", "u.id", "=", "o.user_id")
            .Compile(SqlDialect.SqlServer);

        Assert.Equal(
            "SELECT [u].[id], [o].[user_id] FROM [users] AS [u] JOIN [orders] AS [o] ON [u].[id] = [o].[user_id]",
            sql);
    }

    [Fact]
    public void OracleTableAliases_OmitUnsupportedAsKeyword()
    {
        var sql = new Query()
            .Select("u.id", "o.user_id")
            .From("users", "u")
            .Join("orders", "o", "u.id", "=", "o.user_id")
            .Compile(SqlDialect.Oracle);

        Assert.Equal(
            "SELECT \"u\".\"id\", \"o\".\"user_id\" FROM \"users\" \"u\" JOIN \"orders\" \"o\" ON \"u\".\"id\" = \"o\".\"user_id\"",
            sql);
    }

    [Fact]
    public void SafeJoin_RejectsUnknownOperators()
    {
        var query = new Query();

        var exception = Assert.Throws<ArgumentException>(() =>
            query.Join("orders", "users.id", "= orders.user_id; DROP TABLE users;--", "orders.user_id"));

        Assert.Equal("op", exception.ParamName);
    }

    [Theory]
    [InlineData(-1, "limit")]
    [InlineData(-10, "limit")]
    public void Limit_RejectsNegativeValues(int value, string parameterName)
    {
        var exception = Assert.Throws<ArgumentOutOfRangeException>(() => new Query().Limit(value));
        Assert.Equal(parameterName, exception.ParamName);
    }

    [Fact]
    public void Offset_RejectsNegativeValues()
    {
        var exception = Assert.Throws<ArgumentOutOfRangeException>(() => new Query().Offset(-1));
        Assert.Equal("offset", exception.ParamName);
    }

    [Fact]
    public void Top_RejectsNegativeValues()
    {
        var exception = Assert.Throws<ArgumentOutOfRangeException>(() => new Query().Top(-1));
        Assert.Equal("top", exception.ParamName);
    }
}
