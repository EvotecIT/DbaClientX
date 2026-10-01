using DBAClientX;
using DBAClientX.QueryPlans;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

public sealed class SQLiteQueryPlanTests : IDisposable
{
    private readonly string _database = Path.Combine(Path.GetTempPath(), "dbx-plan-" + Guid.NewGuid().ToString("N") + ".db");
    private readonly SQLite _sqlite = new();

    public SQLiteQueryPlanTests()
    {
        _sqlite.ExecuteNonQuery(_database,
            "CREATE TABLE ProbeResults (Id INTEGER PRIMARY KEY, ProbeName TEXT NOT NULL, Agent TEXT NOT NULL, LatencyMs REAL, SeenUnixMs INTEGER NOT NULL);" +
            "CREATE INDEX IX_ProbeResults_Probe ON ProbeResults (ProbeName, Agent, SeenUnixMs);" +
            "CREATE TABLE Agents (Name TEXT, Site TEXT);" +
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 2000) " +
            "INSERT INTO ProbeResults (ProbeName, Agent, LatencyMs, SeenUnixMs) SELECT 'probe' || (i % 50), 'agent' || (i % 7), i * 0.5, i FROM n;" +
            "INSERT INTO Agents SELECT DISTINCT Agent, 'site' FROM ProbeResults;" +
            "ANALYZE;");
    }

    [Fact]
    public async Task NoFullScan_WhenTheKeyIsWrappedInLower_FailsWithTheSqlAndPlan()
    {
        // The R-62 shape: LOWER() on the columns leaves the composite index unusable, so SQLite reads every row.
        const string sql = "SELECT MAX(SeenUnixMs) FROM ProbeResults WHERE LOWER(ProbeName) = LOWER(@probe) AND LOWER(Agent) = LOWER(@agent)";
        var plan = await _sqlite.ExplainQueryPlanAsync(_database, sql, new Dictionary<string, object?> { ["@probe"] = "Probe1", ["@agent"] = "Agent1" });

        var exception = Assert.Throws<QueryPlanViolationException>(() => QueryPlanAssert.NoFullScan(plan, "ProbeResults"));

        var violation = Assert.Single(exception.Result.Violations);
        Assert.Equal(QueryPlanViolationKind.FullScan, violation.Kind);
        Assert.Equal("ProbeResults", violation.Table);
        Assert.Contains(sql, exception.Message);
        // SQLite prints this full index scan as an unconstrained SEARCH; the step is still classified as a scan.
        Assert.Contains("ProbeResults USING COVERING INDEX IX_ProbeResults_Probe", exception.Message);
        Assert.Equal(DbaQueryPlanOperation.Scan, violation.Step!.Operation);
    }

    [Fact]
    public async Task NoFullScan_WhenTheIndexServesTheCondition_Passes()
    {
        var plan = await _sqlite.ExplainQueryPlanAsync(_database,
            "SELECT MAX(SeenUnixMs) FROM ProbeResults WHERE ProbeName = @probe AND Agent = @agent",
            new Dictionary<string, object?> { ["@probe"] = "probe1", ["@agent"] = "agent1" });

        QueryPlanAssert.UsesIndexes(plan, "ProbeResults");
        var step = Assert.Single(plan.Steps, s => s.Table == "ProbeResults");
        Assert.Equal(DbaQueryPlanOperation.Search, step.Operation);
        Assert.Equal("IX_ProbeResults_Probe", step.Index);
        Assert.True(step.IsCoveringIndex);
    }

    [Fact]
    public async Task UsesIndexes_ReportsATempBTreeOverAScannedTableButNotOverNarrowedRows()
    {
        var scanned = await _sqlite.ExplainQueryPlanAsync(_database, "SELECT Id FROM ProbeResults AS r ORDER BY r.LatencyMs");
        var narrowed = await _sqlite.ExplainQueryPlanAsync(_database, "SELECT Id FROM ProbeResults AS r WHERE r.Id > 10 ORDER BY r.LatencyMs");

        var result = QueryPlanAssert.Check(scanned, new QueryPlanRules("ProbeResults"));

        Assert.Equal(new[] { QueryPlanViolationKind.FullScan, QueryPlanViolationKind.TempBTree }, result.Violations.Select(v => v.Kind).OrderBy(k => k));
        Assert.Equal("ORDER BY", result.Violations.Single(v => v.Kind == QueryPlanViolationKind.TempBTree).Step!.TempBTreePurpose);
        Assert.Contains(narrowed.Steps, s => s.Table == "ProbeResults" && s.Alias == "r" && s.Index == "INTEGER PRIMARY KEY");
        Assert.True(QueryPlanAssert.Check(narrowed, new QueryPlanRules("ProbeResults")).IsSuccess);
    }

    [Theory]
    [InlineData("SELECT Agents.Name FROM Agents LEFT JOIN ProbeResults p ON LOWER(p.Agent) = Agents.Name")]
    [InlineData("SELECT a.Name FROM Agents a, ProbeResults r WHERE LOWER(r.ProbeName) = a.Name")]
    [InlineData("SELECT MAX(SeenUnixMs) FROM main.ProbeResults WHERE LOWER(ProbeName) = 'x'")]
    [InlineData("UPDATE ProbeResults AS r SET LatencyMs = 0 WHERE LOWER(r.Agent) = 'x'")]
    [InlineData("WITH recent AS MATERIALIZED (SELECT * FROM ProbeResults WHERE LOWER(Agent) = 'x') SELECT count(*) FROM recent")]
    public async Task NoFullScan_CatchesScansInJoinsCommaListsSchemasUpdatesAndCtes(string sql)
    {
        var plan = await _sqlite.ExplainQueryPlanAsync(_database, sql);

        var exception = Assert.Throws<QueryPlanViolationException>(() => QueryPlanAssert.NoFullScan(plan, "ProbeResults"));

        Assert.Contains(exception.Result.Violations, v => v.Kind is QueryPlanViolationKind.FullScan or QueryPlanViolationKind.AutomaticIndex);
    }

    [Fact]
    public async Task NoFullScan_OnAWithoutRowIdTable_CatchesTheUnconstrainedPrimaryKeySearch()
    {
        _sqlite.ExecuteNonQuery(_database, "CREATE TABLE Results2 (ProbeName TEXT, Agent TEXT, SeenUnixMs INTEGER, PRIMARY KEY (ProbeName, Agent, SeenUnixMs)) WITHOUT ROWID");
        var plan = await _sqlite.ExplainQueryPlanAsync(_database, "SELECT MAX(SeenUnixMs) FROM Results2 WHERE LOWER(ProbeName) = 'x' AND LOWER(Agent) = 'y'");

        Assert.Throws<QueryPlanViolationException>(() => QueryPlanAssert.NoFullScan(plan, "Results2"));
    }

    [Fact]
    public async Task Check_WhenAnAliasNamesTwoTables_TreatsItsStepsAsReadingEither()
    {
        // ProbeResults also appears under its own name, so only the alias can expose the scan.
        var plan = await _sqlite.ExplainQueryPlanAsync(_database,
            "SELECT * FROM Agents r WHERE EXISTS (SELECT 1 FROM ProbeResults r WHERE LOWER(r.ProbeName) = 'x') " +
            "UNION ALL SELECT * FROM Agents r WHERE r.Name IN (SELECT ProbeName FROM ProbeResults WHERE ProbeName = 'probe1')");

        var result = QueryPlanAssert.Check(plan, new QueryPlanRules("ProbeResults") { RequireLargeTablesInPlan = false });

        Assert.Equal(new[] { "Agents", "ProbeResults" }, plan.AmbiguousAliases["r"].OrderBy(t => t));
        Assert.Contains(result.Violations, v => v.Kind == QueryPlanViolationKind.FullScan && v.Table == "ProbeResults");
    }

    [Fact]
    public async Task Check_WhenANamedTableIsMissing_ReportsIt()
    {
        var plan = await _sqlite.ExplainQueryPlanAsync(_database, "SELECT Name FROM Agents WHERE rowid = 1");

        var result = QueryPlanAssert.Check(plan, new QueryPlanRules("ProbeResult"));

        Assert.Equal(QueryPlanViolationKind.TableNotInPlan, Assert.Single(result.Violations).Kind);
        Assert.True(QueryPlanAssert.Check(plan, new QueryPlanRules("ProbeResult") { RequireLargeTablesInPlan = false }).IsSuccess);
    }

    [Fact]
    public async Task ExplainQueryPlanAsync_RejectsSeveralStatements()
    {
        await Assert.ThrowsAsync<ArgumentException>(() => _sqlite.ExplainQueryPlanAsync(_database, "SELECT 1; CREATE TEMP TABLE x (a)"));
        await Assert.ThrowsAsync<ArgumentException>(() => _sqlite.ExplainQueryPlanAsync(_database, "SELECT $a$; SELECT 1; SELECT $a$"));
        await Assert.ThrowsAsync<ArgumentException>(() => _sqlite.ExplainQueryPlanAsync(_database, "-- nothing to explain\n;"));
        await Assert.ThrowsAsync<ArgumentException>(() => _sqlite.ExplainQueryPlanAsync(_database, "/* c */ EXPLAIN SELECT 1"));
        var plan = await _sqlite.ExplainQueryPlanAsync(_database, ";SELECT ';' AS semicolon; -- trailing comment");
        Assert.NotNull(plan);
    }

    [Fact]
    public async Task Check_ReportsAnAutomaticIndexAndIgnoresSmallTablesNotNamed()
    {
        var plan = await _sqlite.ExplainQueryPlanAsync(_database,
            "SELECT a.Site, r.Id FROM ProbeResults r JOIN Agents a ON a.Name = r.Agent WHERE r.ProbeName = 'probe1'");

        var all = QueryPlanAssert.Check(plan, new QueryPlanRules());

        Assert.Contains(all.Violations, v => v.Table == "Agents" && v.Kind is QueryPlanViolationKind.AutomaticIndex or QueryPlanViolationKind.FullScan);
        Assert.True(QueryPlanAssert.Check(plan, new QueryPlanRules("ProbeResults")).IsSuccess);
    }

    [Fact]
    public async Task ExplainQueryPlanAsync_RejectsAnExplainPrefix()
        => await Assert.ThrowsAsync<ArgumentException>(() => _sqlite.ExplainQueryPlanAsync(_database, "EXPLAIN SELECT 1"));

    [Theory]
    [InlineData("SCAN ProbeResults", DbaQueryPlanOperation.Scan, "ProbeResults", null, false)]
    [InlineData("SCAN r USING COVERING INDEX IX_ProbeResults_Probe", DbaQueryPlanOperation.Scan, "r", "IX_ProbeResults_Probe", true)]
    [InlineData("SCAN Big LEFT-JOIN", DbaQueryPlanOperation.Scan, "Big", null, false)]
    [InlineData("SEARCH b USING AUTOMATIC COVERING INDEX (Agent=?) LEFT-JOIN", DbaQueryPlanOperation.AutomaticIndex, "b", null, true)]
    [InlineData("SEARCH p USING COVERING INDEX IX (Agent=?) LEFT-JOIN", DbaQueryPlanOperation.Search, "p", "IX", true)]
    [InlineData("RIGHT-JOIN Big", DbaQueryPlanOperation.Scan, "Big", null, false)]
    [InlineData("SEARCH W USING PRIMARY KEY", DbaQueryPlanOperation.Scan, "W", "PRIMARY KEY", false)]
    [InlineData("SEARCH W USING PRIMARY KEY (ProbeName=?)", DbaQueryPlanOperation.Search, "W", "PRIMARY KEY", false)]
    [InlineData("SCAN main.ProbeResults", DbaQueryPlanOperation.Scan, "ProbeResults", null, false)]
    [InlineData("SCAN TABLE ProbeResults", DbaQueryPlanOperation.Scan, "ProbeResults", null, false)]
    [InlineData("SCAN 3-ROW VALUES CLAUSE", DbaQueryPlanOperation.Other, null, null, false)]
    [InlineData("SCAN F VIRTUAL TABLE INDEX 0:", DbaQueryPlanOperation.Scan, "F", null, false)]
    [InlineData("SCAN j VIRTUAL TABLE INDEX 1:", DbaQueryPlanOperation.VirtualTable, "j", null, false)]
    [InlineData("SEARCH r USING INDEX IX_ProbeResults_Probe (ProbeName=? AND Agent=?)", DbaQueryPlanOperation.Search, "r", "IX_ProbeResults_Probe", false)]
    [InlineData("SEARCH ProbeResults USING INTEGER PRIMARY KEY (rowid>?)", DbaQueryPlanOperation.Search, "ProbeResults", "INTEGER PRIMARY KEY", false)]
    [InlineData("SEARCH Agents AS a USING AUTOMATIC COVERING INDEX (Name=?)", DbaQueryPlanOperation.AutomaticIndex, "Agents", null, true)]
    [InlineData("SEARCH t USING AUTOMATIC PARTIAL COVERING INDEX (x=?)", DbaQueryPlanOperation.AutomaticIndex, "t", null, true)]
    [InlineData("SCAN HostsSearch VIRTUAL TABLE INDEX 0:M1", DbaQueryPlanOperation.VirtualTable, "HostsSearch", null, false)]
    [InlineData("SEARCH ProbeResults USING COVERING INDEX IX_ProbeResults_Probe", DbaQueryPlanOperation.Scan, "ProbeResults", "IX_ProbeResults_Probe", true)]
    [InlineData("SEARCH ProbeResults", DbaQueryPlanOperation.Scan, "ProbeResults", null, false)]
    [InlineData("SCAN CONSTANT ROW", DbaQueryPlanOperation.Other, null, null, false)]
    [InlineData("CORRELATED SCALAR SUBQUERY 2", DbaQueryPlanOperation.Other, null, null, false)]
    [InlineData("USE TEMP B-TREE FOR GROUP BY", DbaQueryPlanOperation.TempBTree, null, null, false)]
    public void Parser_ClassifiesSqliteDetailText(string detail, DbaQueryPlanOperation operation, string? table, string? index, bool covering)
    {
        var step = SqliteQueryPlanParser.Parse(3, 0, detail);

        Assert.Equal(operation, step.Operation);
        Assert.Equal(table, step.Table);
        Assert.Equal(index, step.Index);
        Assert.Equal(covering, step.IsCoveringIndex);
    }

    public void Dispose()
    {
        _sqlite.Dispose();
        SqliteConnection.ClearAllPools();
        try
        {
            File.Delete(_database);
        }
        catch (IOException)
        {
        }
    }
}
