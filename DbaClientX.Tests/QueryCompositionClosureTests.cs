using DBAClientX.QueryBuilder;
using DBAClientX.QueryPlans;

namespace DbaClientX.Tests;

public sealed class QueryCompositionClosureTests
{
    [Theory]
    [InlineData("UNION")]
    [InlineData("UNION ALL")]
    public void CorrelatedIntersectPrefix_RetainsItsExecutableScope(string followingOperator)
    {
        var child = new Query().SelectRaw("o.Id").Intersect(new Query().Select("1"));
        Append(child, followingOperator, new Query().Select("3"));
        var query = new Query().Select("o.Id").FromRaw("(SELECT 1 AS Id UNION ALL SELECT 2) AS o")
            .WhereIn("o.Id", child).OrderBy("o.Id");
        string sql = query.Compile(SqlDialect.MySql);
        Assert.Contains("INTERSECT SELECT 1 " + followingOperator + " SELECT 3", sql);
    }

    [Theory]
    [InlineData("ARRAY[1,2] AS matrix")]
    [InlineData("ARRAY[[1,2],[3,4]] AS matrix")]
    [InlineData("ARRAY[ARRAY[1,2],ARRAY[3,4]] AS matrix")]
    [InlineData("(ARRAY[[1,2],[3,4]])[1:1] AS matrix")]
    [InlineData("ARRAY['a,b','c,d'] AS matrix")]
    public void PostgreSqlArrayProjection_OrdersByTheActualOutputPosition(string projection)
    {
        Query Operand() => new Query().SelectRaw(projection + ", n.id").FromRaw("(VALUES (2),(1)) AS n(id)");
        string sql = Operand().Union(Operand()).OrderBy("n.id").Limit(1).Compile(SqlDialect.PostgreSql);
        Assert.EndsWith("ORDER BY 2 LIMIT 1", sql);
    }

    [Fact]
    public void SqlServerBracketedIdentifier_RetainsItsProjectionWidth()
    {
        Query Operand() => new Query().SelectRaw("n.[code,part] AS matrix, n.id").From("source", "n");
        Assert.EndsWith("ORDER BY 2", Operand().Union(Operand()).OrderBy("n.id").Compile());
    }

    [Theory]
    [InlineData("WHERE straight_join LIKE '%x' AND LOWER(Name) = 'a'", "straight_join", "Name")]
    [InlineData("WHERE straight_join IN (1,2) AND LOWER(Name) = 'a'", "Name")]
    [InlineData("WHERE straight_join IS NULL AND LOWER(Name) = 'a'", "Name")]
    [InlineData("WHERE straight_join(Name) = 'x' AND LOWER(Other) = 'a'", "Other")]
    [InlineData("WHERE (straight_join LIKE '%x' AND LOWER(Name) = 'a')", "straight_join", "Name")]
    public void StraightJoinPredicate_RetainsConditionScope(string condition, params string[] columns)
    {
        var findings = SqlSargabilityAnalyzer.Analyze("SELECT * FROM t " + condition);
        Assert.Equal(columns, findings.Select(f => f.Column));
        Assert.All(findings, finding => Assert.Equal("t", finding.Table));
    }

    [Fact]
    public void StraightJoinPredicate_InOnCondition_RetainsFollowingFindings()
    {
        var findings = SqlSargabilityAnalyzer.Analyze("SELECT * FROM t JOIN u ON t.id=u.id AND straight_join LIKE '%x' AND LOWER(t.Name)='a'");
        Assert.Equal(new[] { "straight_join", "Name" }, findings.Select(f => f.Column));
        Assert.Equal("t", findings[1].Table);
    }

    private static void Append(Query query, string operation, Query operand)
    {
        switch (operation)
        {
            case "UNION": query.Union(operand); break;
            case "UNION ALL": query.UnionAll(operand); break;
            default: throw new ArgumentOutOfRangeException(nameof(operation));
        }
    }
}
