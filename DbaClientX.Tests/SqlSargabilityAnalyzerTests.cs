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
    // The outermost function around the column is the expression an index would need; calls inside it are part of it.
    [InlineData("SELECT * FROM t WHERE instr(dbx_lower(\"Name\"), @p0) > 0 AND DBX_UPPER(t.Code) = @p1", "instr(dbx_lower(\"Name\"), @p0)", "DBX_UPPER(t.Code)")]
    [InlineData("SELECT * FROM t WHERE dbx_lower(NULLIF(\"Name\", '')) = @p0", "dbx_lower(NULLIF(\"Name\", ''))")]
    [InlineData("SELECT * FROM t WHERE my_unknown(LOWER(Name)) = 1 AND ABS(@p - Seen) < 5", "LOWER(Name)", "ABS(@p - Seen)")]
    [InlineData("SELECT * FROM t WHERE LOWER((SELECT Name FROM u WHERE u.Id = 1)) = 'x' AND ROUND(DATEDIFF(day, Created, @p)) > 1", "ROUND(DATEDIFF(day, Created, @p))")]
    [InlineData("SELECT * FROM t WHERE LOWER(Zone) = @p AND IFNULL(Zone, x'') = @z AND ABS(@p::int - Seen) < 5 AND CONVERT(NVARCHAR(MAX), Name) = @n", "LOWER(Zone)", "IFNULL(Zone, x'')", "ABS(@p::int - Seen)", "CONVERT(NVARCHAR(MAX), Name)")]
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
    [InlineData("SELECT * FROM t WHERE CAST(Name AS TEXT) COLLATE NOCASE = @p", "CAST(Name AS TEXT) COLLATE NOCASE")]
    [InlineData("SELECT * FROM t WHERE CAST(t.Name AS TEXT) = @p COLLATE NOCASE", "CAST(t.Name AS TEXT) = @p COLLATE NOCASE")]
    [InlineData("SELECT * FROM t WHERE @p COLLATE NOCASE = CAST(t.Name AS TEXT)", "@p COLLATE NOCASE = CAST(t.Name AS TEXT)")]
    [InlineData("SELECT * FROM t WHERE dbx_lower(NULLIF(Name, '')) COLLATE DBX_NOCASE >= @p", "dbx_lower(NULLIF(Name, '')) COLLATE DBX_NOCASE")]
    [InlineData("SELECT * FROM t WHERE COALESCE(CASE WHEN t.Name > '' THEN t.Name END, '') COLLATE NOCASE = @p", "COALESCE(CASE WHEN t.Name > '' THEN t.Name END, '') COLLATE NOCASE")]
    public void Analyze_ReportsTheCollationOfAFunctionAroundAColumnBesidesTheFunction(string sql, string collation)
    {
        var findings = SqlSargabilityAnalyzer.Analyze(sql);

        var collated = Assert.Single(findings, f => f.Kind == SqlSargabilityFindingKind.CollationOnColumn);
        Assert.Equal(collation, collated.Text);
        Assert.Equal("Name", collated.Column);
        Assert.Equal("t", collated.Table);
        Assert.Single(findings, f => f.Kind == SqlSargabilityFindingKind.FunctionOnColumn);
    }

    [Theory]
    [InlineData("SELECT * FROM ProbeResults p JOIN Agents a ON a.Name = p.Agent WHERE LOWER(p.ProbeName) = @x", "ProbeName", "ProbeResults")]
    [InlineData("SELECT * FROM main.ProbeResults WHERE EXTRACT(YEAR FROM Seen) = 2026 AND LOWER(ProbeName) = @x", "ProbeName", "ProbeResults")]
    [InlineData("SELECT * FROM ProbeResults, Agents WHERE LOWER(ProbeName) = @x", "ProbeName", null)]
    [InlineData("SELECT * FROM t WHERE x IN (SELECT y FROM u WHERE trim(Name) = 'a')", "Name", "u")]
    [InlineData("SELECT * FROM t o WHERE EXISTS (SELECT 1 FROM u WHERE LOWER(o.Name) = u.Name)", "Name", "t")]
    [InlineData("SELECT * FROM (SELECT Name FROM t) d WHERE LOWER(Name) = 'x'", "Name", null)]
    [InlineData("UPDATE ProbeResults AS r SET LatencyMs = 0 WHERE r.Agent::text = 'x'", "Agent", "ProbeResults")]
    [InlineData("SELECT * FROM a WHERE Name = 'x' UNION SELECT * FROM b WHERE b.Name NOT LIKE '%x'", "Name", "b")]
    public void Analyze_NamesTheColumnAndItsTable(string sql, string column, string? table)
    {
        // The last finding; EXTRACT(YEAR FROM Seen) is one too, and its FROM must not count as a table.
        var finding = SqlSargabilityAnalyzer.Analyze(sql).Last();

        Assert.Equal(column, finding.Column);
        Assert.Equal(table, finding.Table);
        Assert.Contains(table == null ? $"({column})" : $"({table}.{column})", finding.Message);
    }

    [Theory]
    [InlineData("WITH d AS (SELECT Name FROM t) SELECT * FROM d WHERE LOWER(Name) = 'x'", null)]
    [InlineData("SELECT * FROM Agents j WHERE EXISTS (SELECT 1 FROM json_each(@p) j WHERE LOWER(j.value) = 'x')", null)]
    [InlineData("UPDATE r SET Latency = 0 FROM ProbeResults r WHERE LOWER(r.Agent) = 'x'", "ProbeResults")]
    [InlineData("SELECT * FROM t WITH (NOLOCK), u WHERE LOWER(Name) = 'x'", null)]
    [InlineData("INSERT INTO archive SELECT * FROM ProbeResults WHERE LOWER(Agent) = 'x'", "ProbeResults")]
    [InlineData("DELETE FROM ProbeResults WHERE a IS NOT DISTINCT FROM b AND LOWER(Agent) = 'x'", "ProbeResults")]
    public void Analyze_TracesTablesOnlyToStoredSources(string sql, string? table)
    {
        var finding = SqlSargabilityAnalyzer.Analyze(sql).Last();

        Assert.Equal(SqlSargabilityFindingKind.FunctionOnColumn, finding.Kind);
        Assert.Equal(table, finding.Table);
    }

    [Theory]
    [InlineData("SELECT * FROM t WHERE Name = CONVERT(@p USING utf8mb4) AND Code = CAST(@p AS NVARCHAR(MAX)) AND Raw = CONVERT(NVARCHAR(MAX), @p) AND Z = IFNULL(@p, x'')")]
    [InlineData("SELECT * FROM t WHERE x = CAST(@p AS DOUBLE PRECISION) AND y = CAST(@p AS TIMESTAMP WITH TIME ZONE)")]
    [InlineData("SELECT * FROM t WHERE d = DATE_TRUNC('day', @p::timestamptz) AND e = DATE(@p AT TIME ZONE 'UTC')")]
    [InlineData("SELECT * FROM t WHERE d > DATE(NOW() - INTERVAL 7 DAY) AND s = SUBSTRING(@p FROM 1 FOR 3)")]
    [InlineData("SELECT * FROM t WHERE n = LOWER(@p COLLATE pg_catalog.\"C\") AND k = CAST(EXISTS (SELECT 1 FROM u WHERE u.Id = t.Id) AS INTEGER)")]
    [InlineData("SELECT * FROM t WHERE (SELECT Name FROM u LIMIT 1) COLLATE NOCASE = @p AND (CASE WHEN Flag = 1 THEN Name END) COLLATE NOCASE = @q")]
    public void Analyze_IgnoresKeywordsTypesAndSubqueriesInsideCalls(string sql)
        => Assert.Empty(SqlSargabilityAnalyzer.Analyze(sql));

    [Fact]
    public void Analyze_ReportsAColumnUnderAnUnknownFunctionAndTheSubqueriesInsideAReportedCall()
    {
        var findings = SqlSargabilityAnalyzer.Analyze(
            "SELECT * FROM t WHERE LOWER(my_unknown(Name)) = 'x' AND COALESCE((SELECT MAX(v) FROM u WHERE TRIM(u.Code) = 'a'), Seen) > 0 " +
            "AND ABS(CASE WHEN Flag = 1 THEN Latency ELSE 0 END) > 5 AND Id = 1 AND (Name) COLLATE NOCASE = @p");

        Assert.Equal(
            new[] { "(Name) COLLATE NOCASE", "ABS(CASE WHEN Flag = 1 THEN Latency ELSE 0 END)", "COALESCE((SELECT MAX(v) FROM u WHERE TRIM(u.Code) = 'a'), Seen)", "LOWER(my_unknown(Name))", "TRIM(u.Code)" },
            findings.Select(f => f.Text).OrderBy(t => t, StringComparer.Ordinal));
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