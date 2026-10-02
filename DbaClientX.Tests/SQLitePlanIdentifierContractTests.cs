using DBAClientX;
using DBAClientX.QueryPlans;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

public sealed class SQLitePlanIdentifierContractTests : IDisposable
{
    private readonly string _database = Path.Combine(Path.GetTempPath(), "dbx-identifiers-" + Guid.NewGuid().ToString("N") + ".db");
    private readonly SQLite _sqlite = new();

    [Fact]
    public async Task StatisticsAndEstimates_NonAsciiNames_RemainDistinct()
    {
        _sqlite.ExecuteNonQuery(_database, "CREATE TABLE ä (v INTEGER); CREATE TABLE Ä (v INTEGER);" +
            "CREATE INDEX ix_ä ON ä(v); CREATE INDEX ix_Ä ON Ä(v);");
        await _sqlite.WritePlannerStatisticsAsync(_database, new[] {
            new SqlitePlannerStatistics("ä", "ix_ä", 1000, new long[] { 100 }),
            new SqlitePlannerStatistics("Ä", "ix_Ä", 2000, new long[] { 500 })
        });

        var statistics = await _sqlite.ReadPlannerStatisticsAsync(_database);
        Assert.Equal(2, statistics.Count);
        foreach (var expected in new[] { (Table: "ä", Index: "ix_ä", Rows: 1000L, Estimate: 100L), (Table: "Ä", Index: "ix_Ä", Rows: 2000L, Estimate: 500L) })
        {
            Assert.Equal(expected.Rows, Assert.Single(statistics, s => s.Table == expected.Table).RowCount);
            var plan = await _sqlite.ExplainQueryPlanAsync(_database, $"SELECT v FROM {expected.Table} INDEXED BY {expected.Index} WHERE v = 1");
            var step = Assert.Single(plan.Steps, s => s.Table == expected.Table);
            Assert.Equal(expected.Rows, step.TableRows);
            Assert.Equal(expected.Estimate, step.EstimatedRows);
            Assert.True(QueryPlanAssert.Check(plan, new QueryPlanRules(expected.Table == "ä" ? "Ä" : "ä") { RequireLargeTablesInPlan = false }).IsSuccess);
        }
    }

    [Theory]
    [InlineData("SELECT * FROM \"audit.events\"", null)]
    [InlineData("SELECT * FROM main.\"audit.events\"", null)]
    [InlineData("SELECT * FROM \"audit.events\" AS \"p.q\"", "p.q")]
    [InlineData("SELECT * FROM \"audit.events\" AS \"WHERE\"", "WHERE")]
    public async Task ExplainQueryPlan_DottedQuotedNames_KeepRuleAttribution(string sql, string? alias)
    {
        _sqlite.ExecuteNonQuery(_database, "CREATE TABLE \"audit.events\" (v TEXT); INSERT INTO \"audit.events\" VALUES ('event'); ANALYZE;");
        var plan = await _sqlite.ExplainQueryPlanAsync(_database, sql);
        var step = Assert.Single(plan.FullScans);
        Assert.Equal("audit.events", step.Table);
        Assert.Equal(alias, step.Alias);
        Assert.Equal(1L, step.TableRows);
        var failure = QueryPlanAssert.Check(plan, new QueryPlanRules("audit.events") { RequireLargeTablesInPlan = false });
        Assert.Equal(QueryPlanViolationKind.FullScan, Assert.Single(failure.Violations).Kind);
        Assert.Equal("audit.events", failure.Violations[0].Table);
    }

    [Fact]
    public async Task ExplainQueryPlan_NonAsciiColumnCase_DoesNotInventAMinMaxSeek()
    {
        _sqlite.ExecuteNonQuery(_database, "CREATE TABLE Names (ä INTEGER, Ä INTEGER); CREATE INDEX ix_names ON Names(ä);" +
            "INSERT INTO Names VALUES(1,2),(3,4); ANALYZE;");
        var plan = await _sqlite.ExplainQueryPlanAsync(_database, "SELECT MAX(Ä) FROM Names");
        QueryPlanAssert.NoFullScan(await _sqlite.ExplainQueryPlanAsync(_database, "SELECT MAX(ä) FROM Names"), "Names");
        Assert.Throws<QueryPlanViolationException>(() => QueryPlanAssert.NoFullScan(plan, "Names"));
        Assert.DoesNotContain(plan.Steps, s => s.EstimatedRows == 1 && s.Operation == DbaQueryPlanOperation.Search);
    }

    [Theory]
    [InlineData("SELECT MAX(Ä) FROM Names INDEXED BY ix_names WHERE ä > 0")]
    [InlineData("SELECT Id FROM Names INDEXED BY ix_names WHERE ä > 0 AND Ä = 999999 LIMIT 10")]
    public async Task UsesIndexes_NonAsciiResidualColumn_DoesNotHideWideWork(string sql)
    {
        _sqlite.ExecuteNonQuery(_database, "CREATE TABLE Names (Id INTEGER PRIMARY KEY, ä INTEGER, Ä INTEGER); CREATE INDEX ix_names ON Names(ä);");
        await _sqlite.WritePlannerStatisticsAsync(_database, new[] { new SqlitePlannerStatistics("Names", "ix_names", 1_000_000, new long[] { 500_000 }) });
        var plan = await _sqlite.ExplainQueryPlanAsync(_database, sql);
        Assert.Contains(QueryPlanAssert.Check(plan, new QueryPlanRules("Names")).Violations, v => v.Kind == QueryPlanViolationKind.WideSearch);
    }

    [Fact]
    public async Task ExplainQueryPlan_QualifiedQuotedKeyword_ResolvesTheTable()
    {
        _sqlite.ExecuteNonQuery(_database, "CREATE TABLE \"order\" (v TEXT); INSERT INTO \"order\" VALUES ('event'); ANALYZE;");
        var plan = await _sqlite.ExplainQueryPlanAsync(_database, "SELECT * FROM main.\"order\"");
        Assert.Equal("order", Assert.Single(plan.FullScans).Table);
        Assert.Contains(QueryPlanAssert.Check(plan, new QueryPlanRules("order") { RequireLargeTablesInPlan = false }).Violations,
            v => v.Table == "order" && v.Kind == QueryPlanViolationKind.FullScan);
    }

    [Fact]
    public async Task ExplainQueryPlan_CollidingDisplayLabels_KeepsEveryCandidateWithoutBorrowingEstimates()
    {
        _sqlite.ExecuteNonQuery(_database, "CREATE TABLE events (v TEXT); CREATE TABLE \"main.events\" (v TEXT);" +
            "INSERT INTO events VALUES ('a'); INSERT INTO \"main.events\" VALUES ('b'),('c'); ANALYZE;");
        var plan = await _sqlite.ExplainQueryPlanAsync(_database, "SELECT * FROM main.events UNION ALL SELECT * FROM \"main.events\"");
        var ambiguous = plan.Steps.Where(s => s.Table == "main.events").ToArray();
        Assert.Equal(2, ambiguous.Length);
        Assert.All(ambiguous, step => {
            Assert.Equal(new[] { "events", "main.events" }, plan.TablesOf(step).OrderBy(t => t));
            Assert.Null(step.TableRows);
        });
        foreach (var table in new[] { "events", "main.events" })
            Assert.Contains(QueryPlanAssert.Check(plan, new QueryPlanRules(table) { RequireLargeTablesInPlan = false }).Violations,
                v => v.Table == table && v.Kind == QueryPlanViolationKind.FullScan);
    }

    [Fact]
    public async Task ExplainQueryPlan_ResolvedTableAlsoUsedAsAnAlias_EnrichmentKeepsOriginalIdentity()
    {
        _sqlite.ExecuteNonQuery(_database, "CREATE TABLE events (v TEXT); CREATE TABLE Agents (v TEXT);" +
            "INSERT INTO events VALUES ('a'); INSERT INTO Agents VALUES ('b'),('c'); ANALYZE;");
        var plan = await _sqlite.ExplainQueryPlanAsync(_database, "SELECT * FROM main.events UNION ALL SELECT * FROM Agents AS events");
        var original = Assert.Single(plan.FullScans, s => s.Table == "events");
        Assert.Null(original.Alias);
        Assert.Equal(1L, original.TableRows);
        var aliased = Assert.Single(plan.FullScans, s => s.Table == "Agents");
        Assert.Equal("events", aliased.Alias);
        Assert.Equal(2L, aliased.TableRows);
        Assert.Contains(QueryPlanAssert.Check(plan, new QueryPlanRules("events") { RequireLargeTablesInPlan = false }).Violations,
            v => v.Kind == QueryPlanViolationKind.FullScan && v.Table == "events");
    }

    public void Dispose()
    {
        _sqlite.Dispose();
        SqliteConnection.ClearAllPools();
        if (File.Exists(_database)) File.Delete(_database);
    }
}
