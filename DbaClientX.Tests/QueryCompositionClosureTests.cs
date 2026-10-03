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

    [Theory]
    [InlineData("u.id + straight_join(t.Name)")]
    [InlineData("straight_join(t.Name)")]
    [InlineData("BINARY straight_join(t.Name)")]
    [InlineData("u.id DIV straight_join(t.Name)")]
    [InlineData("u.id XOR straight_join(t.Name)")]
    public void StraightJoinFunction_InOnExpression_RetainsFollowingFindings(string expression)
    {
        var findings = SqlSargabilityAnalyzer.Analyze("SELECT * FROM t JOIN u ON t.id = " + expression + " AND LOWER(t.Other) = 'a'");
        var finding = Assert.Single(findings);
        Assert.Equal("Other", finding.Column);
        Assert.Equal("t", finding.Table);
    }

    [Fact]
    public void StraightJoinSource_AfterOnExpression_StillResolvesItsTable()
    {
        var findings = SqlSargabilityAnalyzer.Analyze("SELECT * FROM t JOIN u ON t.id = u.id STRAIGHT_JOIN v ON v.id = t.id AND LOWER(v.Name) = 'a'");
        var finding = Assert.Single(findings);
        Assert.Equal("Name", finding.Column);
        Assert.Equal("v", finding.Table);
    }

    [Theory]
    [InlineData("q'!a'b,c!'", false)]
    [InlineData("q'!a'b,c!'", true)]
    [InlineData("Q'[a'b,c]'", false)]
    [InlineData("q'{a'b,c}'", false)]
    [InlineData("q'(a'b,c)'", false)]
    [InlineData("q'<a'b,c>'", false)]
    [InlineData("nQ'\u00ef a'b,c \u00ef'", false)]
    public void OracleAlternativeQuotedProjection_OrdersByTheActualOutputPosition(string literal, bool separateExpressions)
    {
        Query Operand()
        {
            var query = new Query();
            if (separateExpressions) query.SelectRaw(literal + " AS label").SelectRaw("n.id");
            else query.SelectRaw(literal + " AS label, n.id");
            return query.FromRaw("(SELECT 1 AS id FROM dual) n");
        }
        Assert.EndsWith("ORDER BY 2", Operand().Union(Operand()).OrderBy("n.id").Compile(SqlDialect.Oracle));
    }

    [Theory]
    [InlineData("q'!a'b; c,d!'")]
    [InlineData("nQ'[a'b; c,d]'")]
    [InlineData("q'\U0001F642a'b; c,d\U0001F642'")]
    public void OracleAlternativeQuotedStatement_KeepsLiteralSemicolonsInsideTheStatement(string literal)
    {
        string statement = "SELECT " + literal + " AS label FROM dual";
        Assert.Equal(new[] { statement, "SELECT 2 FROM dual" }, SqlStatementText.Split(statement + "; SELECT 2 FROM dual;", SqlDialect.Oracle));
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
