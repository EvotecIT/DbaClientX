using DBAClientX.QueryPlans;

namespace DbaClientX.Tests;

public sealed class SqlSargabilityAnalyzerTests
{
    [Theory]
    [InlineData("SELECT * FROM ProbeResults WHERE LOWER(ProbeName) = LOWER(@p) AND LOWER(Agent) = LOWER(@a)", "LOWER(ProbeName)", "LOWER(Agent)")]
    [InlineData("SELECT * FROM t JOIN u ON UPPER(t.\"Code\") = u.Code", "UPPER(t.\"Code\")")]
    [InlineData("SELECT * FROM t WHERE CAST([Created] AS date) = @d", "CAST([Created] AS date)")]
    [InlineData("SELECT * FROM t WHERE x IN (SELECT y FROM u WHERE trim(u.Name) = 'a')", "trim(u.Name)")]
    [InlineData("SELECT * FROM t WHERE strftime('%Y', Seen) = '2026'", "strftime('%Y', Seen)")]
    [InlineData("UPDATE t SET a = 1 WHERE COALESCE(Deleted, 0) = 0", "COALESCE(Deleted, 0)")]
    [InlineData("SELECT * FROM t WHERE CASE WHEN lower(Name) = 'a' THEN 1 END = 1", "lower(Name)")]
    [InlineData("SELECT * FROM t WHERE Created::date = @d", "Created::date")]
    [InlineData("SELECT * FROM p JOIN (SELECT x FROM s) d ON LOWER(d.x) = p.y", "LOWER(d.x)")]
    [InlineData("SELECT * FROM t WHERE CONVERT(Name USING utf8mb4) = @p AND CONVERT(Created, DATE) = @d", "CONVERT(Name USING utf8mb4)", "CONVERT(Created, DATE)")]
    [InlineData("SELECT * FROM t WHERE EXTRACT(YEAR FROM Seen) = 2026", "EXTRACT(YEAR FROM Seen)")]
    public void Analyze_ReportsFunctionsWrappedAroundColumnsInConditions(string sql, params string[] expected)
    {
        var findings = SqlSargabilityAnalyzer.Analyze(sql);

        Assert.Equal(expected, findings.Where(f => f.Kind == SqlSargabilityFindingKind.FunctionOnColumn).Select(f => f.Text));
    }

    [Theory]
    [InlineData("SELECT * FROM t WHERE Name LIKE '%warsaw%'", "LIKE '%warsaw%'")]
    [InlineData("SELECT * FROM t WHERE Name NOT LIKE N'_x'", "LIKE N'_x'")]
    [InlineData("SELECT * FROM t WHERE Name GLOB '*x'", "GLOB '*x'")]
    public void Analyze_ReportsLeadingWildcards(string sql, string expected)
    {
        var finding = Assert.Single(SqlSargabilityAnalyzer.Analyze(sql));

        Assert.Equal(SqlSargabilityFindingKind.LeadingWildcard, finding.Kind);
        Assert.Equal(expected, finding.Text);
        Assert.Equal(sql.IndexOf(expected, StringComparison.Ordinal), finding.Position);
    }

    [Theory]
    [InlineData("SELECT LOWER(Name), COUNT(*) FROM t WHERE Name = @p GROUP BY LOWER(Name) ORDER BY UPPER(Name)")]
    [InlineData("SELECT * FROM t WHERE Name = LOWER(@p) AND Code = UPPER('x')")]
    [InlineData("SELECT * FROM t WHERE Name LIKE 'war%' AND Code GLOB 'a*'")]
    [InlineData("SELECT * FROM t WHERE Name = 'LOWER(Name) = 1' -- LOWER(Name) = 2\n/* WHERE LOWER(Name) = 3 */")]
    [InlineData("SELECT * FROM t WHERE \"LOWER\" = 1 AND Seen > datetime('now')")]
    [InlineData("SELECT COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 1")]
    [InlineData("SELECT * FROM t WHERE Name LIKE @pattern")]
    [InlineData("SELECT * FROM t WHERE Created > DATEADD(day, -7, GETDATE()) AND Day >= CONVERT(date, @p) AND Y = EXTRACT(YEAR FROM @p)")]
    [InlineData("CREATE INDEX IX_Name ON t (LOWER(Name)); INSERT INTO t VALUES (1) ON CONFLICT (Id) DO NOTHING")]
    [InlineData("CREATE TABLE c (Id INTEGER REFERENCES p (Id) ON DELETE CASCADE, Name TEXT CHECK (LENGTH(Name) > 0))")]
    public void Analyze_IgnoresIndexablePredicatesProjectionsLiteralsAndComments(string sql)
        => Assert.Empty(SqlSargabilityAnalyzer.Analyze(sql));
}
