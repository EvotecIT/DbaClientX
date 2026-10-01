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
    [InlineData("SELECT * FROM t WHERE instr(dbx_lower(\"Name\"), @p0) > 0 AND DBX_UPPER(t.Code) = @p1", "dbx_lower(\"Name\")", "DBX_UPPER(t.Code)")]
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

    [Theory]
    [InlineData("SELECT * FROM t WHERE \"Name\" COLLATE DBX_NOCASE >= @p0", "\"Name\" COLLATE DBX_NOCASE")]
    [InlineData("SELECT * FROM t WHERE t.Name COLLATE NOCASE = @p0 AND Id > 1", "t.Name COLLATE NOCASE")]
    [InlineData("SELECT * FROM t WHERE Name = @p0 COLLATE NOCASE", "Name = @p0 COLLATE NOCASE")]
    [InlineData("SELECT * FROM t WHERE Name <> 'x' COLLATE \"C\"", "Name <> 'x' COLLATE \"C\"")]
    [InlineData("SELECT * FROM a JOIN b ON a.Code COLLATE Latin1_General_CI_AS = b.Code", "a.Code COLLATE Latin1_General_CI_AS")]
    [InlineData("SELECT * FROM t WHERE @p COLLATE NOCASE = t.[Name]", "@p COLLATE NOCASE = t.[Name]")]
    [InlineData("SELECT * FROM t WHERE Name = @p COLLATE pg_catalog.\"C\"; SELECT 1", "Name = @p COLLATE pg_catalog.\"C\"")]
    [InlineData("SELECT * FROM t WHERE Id = 1 AND Name COLLATE", null)]
    [InlineData("SELECT * FROM t WHERE @p COLLATE NOCASE = s.t.Name", "@p COLLATE NOCASE = s.t.Name")]
    [InlineData("SELECT * FROM t WHERE [dbo].[t].[Name] COLLATE NOCASE = @p", "[dbo].[t].[Name] COLLATE NOCASE")]
    [InlineData("SELECT * FROM t WHERE Name COLLATE NOCASE NOT NULL AND Code = @p COLLATE pg_catalog.\"default\"", null)]
    public void Analyze_ReportsCollationsAppliedToColumnComparisons(string sql, string? expected)
    {
        if (expected == null)
        {
            Assert.Empty(SqlSargabilityAnalyzer.Analyze(sql));
            return;
        }

        var finding = Assert.Single(SqlSargabilityAnalyzer.Analyze(sql));

        Assert.Equal(SqlSargabilityFindingKind.CollationOnColumn, finding.Kind);
        Assert.Equal(expected, finding.Text);
        Assert.Equal(sql.IndexOf(expected, StringComparison.Ordinal), finding.Position);
    }

    [Theory]
    [InlineData("SELECT * FROM t ORDER BY Name COLLATE NOCASE")]
    [InlineData("SELECT Name COLLATE NOCASE FROM t WHERE Id = 1")]
    [InlineData("SELECT * FROM t WHERE NameFolded COLLATE dbx_nocase > @p AND t.\"NameFolded\" COLLATE DBX_NOCASE < @q")]
    [InlineData("SELECT * FROM t WHERE x IN (SELECT y FROM u) AND CASE WHEN a = 1 THEN 'a' END COLLATE NOCASE = 'a'")]
    [InlineData("SELECT \"Site\" COLLATE BINARY, COUNT(*) FROM t WHERE +\"Site\" COLLATE BINARY IS NOT NULL AND \"Name\" COLLATE NOCASE IS NULL GROUP BY 1")]
    [InlineData("SELECT * FROM t WHERE Name = @p COLLATE BINARY AND Code COLLATE NOCASE NOTNULL AND x IN (@a, @b COLLATE NOCASE)")]
    public void Analyze_IgnoresCollationsOutsideConditionsAndOnesTheIndexUses(string sql)
    {
        var options = new SqlSargabilityOptions();
        options.ColumnCollations["NameFolded"] = "DBX_NOCASE";

        Assert.Empty(SqlSargabilityAnalyzer.Analyze(sql, options));
    }

    [Fact]
    public void Analyze_WithRegisteredFunctions_ReportsThemAroundColumns()
    {
        const string sql = "SELECT * FROM t WHERE my_fold(Name) = @p AND Code = my_fold(@q)";
        var options = new SqlSargabilityOptions();
        options.Functions.Add("MY_FOLD");

        Assert.Empty(SqlSargabilityAnalyzer.Analyze(sql));
        Assert.Equal(new[] { "my_fold(Name)" }, SqlSargabilityAnalyzer.Analyze(sql, options).Select(f => f.Text));
        Assert.Single(SqlSargabilityAnalyzer.Analyze("SELECT * FROM t WHERE Name COLLATE BINARY = @p", new SqlSargabilityOptions { DefaultCollation = null }));
    }
}