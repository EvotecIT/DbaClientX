using DBAClientX;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

public sealed class SQLitePlannerStatisticsTests : IDisposable
{
    private readonly string _database = Path.Combine(Path.GetTempPath(), "dbx-stat-" + Guid.NewGuid().ToString("N") + ".db");
    private readonly SQLite _sqlite = new();

    public SQLitePlannerStatisticsTests()
    {
        _sqlite.ExecuteNonQuery(_database,
            "CREATE TABLE Results (Id INTEGER PRIMARY KEY, Status INTEGER NOT NULL, Agent TEXT NOT NULL, Seen INTEGER NOT NULL);" +
            "CREATE INDEX IX_Results_Status ON Results (Status, Seen);" +
            "CREATE INDEX IX_Results_Agent ON Results (Agent);" +
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 200) " +
            "INSERT INTO Results (Status, Agent, Seen) SELECT i % 3, 'agent' || i, i FROM n;");
    }

    [Fact]
    public async Task WritePlannerStatisticsAsync_MakesEveryConnectionPlanWithTheWrittenRows()
    {
        const string query = "SELECT Id FROM Results WHERE Status = 1 AND Agent = 'agent7'";
        await _sqlite.WritePlannerStatisticsAsync(_database, new[]
        {
            new SqlitePlannerStatistics("Results", "IX_Results_Status", 1_000_000, new long[] { 2, 1 }),
            new SqlitePlannerStatistics("Results", "IX_Results_Agent", 1_000_000, new long[] { 500_000 })
        });

        // A connection that stays open across the next write, as a pooled one does, must plan with its statistics too.
        await using var early = new SqliteConnection(SQLite.BuildConnectionString(_database));
        await early.OpenAsync();
        Assert.Contains("IX_Results_Status", await PlanAsync(early, query));

        // Now status matches a third of a million rows and an agent one row.
        await _sqlite.WritePlannerStatisticsAsync(_database, new[]
        {
            new SqlitePlannerStatistics("Results", "IX_Results_Status", 1_000_000, new long[] { 333_334, 1 }),
            new SqlitePlannerStatistics("Results", "IX_Results_Agent", 1_000_000, new long[] { 1 })
        });

        // EXPLAIN QUERY PLAN alone does not read the database, so it still plans with the old statistics; the next
        // statement that reads finds the schema changed and reloads it with the new ones.
        Assert.Contains("IX_Results_Status", await PlanAsync(early, query));
        await using (var read = early.CreateCommand())
        {
            read.CommandText = "SELECT COUNT(*) FROM Results WHERE Id = 1";
            await read.ExecuteScalarAsync();
        }

        Assert.Contains("IX_Results_Agent", await PlanAsync(early, query));
        var plan = await _sqlite.ExplainQueryPlanAsync(_database, query);
        Assert.Equal("IX_Results_Agent", Assert.Single(plan.Steps, s => s.Table == "Results").Index);
    }

    [Fact]
    public async Task ReadAndWritePlannerStatistics_RoundTripAndReplace()
    {
        Assert.Empty(await _sqlite.ReadPlannerStatisticsAsync(_database));
        var status = new SqlitePlannerStatistics("Results", "IX_Results_Status", 5000, new long[] { 1700, 2 }, new[] { "noskipscan" });
        await _sqlite.WritePlannerStatisticsAsync(_database, new[] { status, new SqlitePlannerStatistics("Results", null, 5000) });

        var read = await _sqlite.ReadPlannerStatisticsAsync(_database);

        Assert.Equal(new[] { "Results (table): 5000", "Results IX_Results_Status: 5000 1700 2 noskipscan" }, read.Select(r => r.ToString()).OrderBy(t => t));

        // A row replaces the row of the same table and index; replaceAll leaves only the given rows.
        await _sqlite.WritePlannerStatisticsAsync(_database, new[] { new SqlitePlannerStatistics("Results", "IX_Results_Status", 9000, new long[] { 3000, 1 }) });
        Assert.Contains(await _sqlite.ReadPlannerStatisticsAsync(_database), r => r.Index == "IX_Results_Status" && r.RowCount == 9000);
        Assert.Equal(2, (await _sqlite.ReadPlannerStatisticsAsync(_database)).Count);
        // Names compare without case, as SQLite matches them; of rows naming the same index, the last one written counts.
        await _sqlite.WritePlannerStatisticsAsync(_database, new[] { new SqlitePlannerStatistics("RESULTS", "ix_results_status", 7000, new long[] { 2000, 1 }) });
        Assert.Equal(7000, Assert.Single(await _sqlite.ReadPlannerStatisticsAsync(_database), r => r.Index != null).RowCount);
        _sqlite.ExecuteNonQuery(_database, "INSERT INTO sqlite_stat1 VALUES ('Results', 'IX_Results_Status', '8000 2500 1')");
        Assert.Equal(8000, Assert.Single(await _sqlite.ReadPlannerStatisticsAsync(_database), r => r.Index != null).RowCount);
        await _sqlite.WritePlannerStatisticsAsync(_database, Array.Empty<SqlitePlannerStatistics>(), replaceAll: true);
        Assert.Empty(await _sqlite.ReadPlannerStatisticsAsync(_database));

        // With options, SQLite would read missing key counts as unique prefixes.
        await Assert.ThrowsAsync<ArgumentException>(() => _sqlite.WritePlannerStatisticsAsync(_database,
            new[] { new SqlitePlannerStatistics("Results", "IX_Results_Status", 9000, new long[] { 3000 }, new[] { "noskipscan" }) }));
    }

    [Fact]
    public async Task ReadPlannerStatisticsAsync_ReadsWhatAnalyzeWrites()
    {
        _sqlite.ExecuteNonQuery(_database, "ANALYZE;");

        var status = Assert.Single(await _sqlite.ReadPlannerStatisticsAsync(_database), r => r.Index == "IX_Results_Status");

        Assert.Equal(200, status.RowCount);
        Assert.Equal(new long[] { 67, 1 }, status.RowsPerKey);
    }

    [Theory]
    [InlineData("1000 10 1", 1000L, new long[] { 10, 1 }, new string[0])]
    [InlineData("1000 0 unordered sz=12", 1000L, new long[] { 1 }, new[] { "unordered", "sz=12" })]
    [InlineData("1000.0 10 unordered 5", 1000L, new long[] { 10 }, new[] { "unordered" })]
    [InlineData("99999999999999999999 1", long.MaxValue, new long[] { 1 }, new string[0])]
    public void Parse_ReadsCountsAndOptions(string stat, long rows, long[] perKey, string[] options)
    {
        var parsed = SqlitePlannerStatistics.Parse("t", "ix", stat);

        Assert.Equal(rows, parsed.RowCount);
        Assert.Equal(perKey, parsed.RowsPerKey);
        Assert.Equal(options, parsed.Options);
    }

    [Fact]
    public void Constructor_RejectsRowsSqliteCannotStore()
    {
        Assert.Throws<ArgumentException>(() => new SqlitePlannerStatistics("t", null, 10, new long[] { 1 }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new SqlitePlannerStatistics("t", "ix", -1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new SqlitePlannerStatistics("t", "ix", 10, new long[] { 0 }));
        Assert.Throws<ArgumentException>(() => new SqlitePlannerStatistics("t", "ix", 10, options: new[] { "two words" }));
        Assert.Throws<ArgumentException>(() => new SqlitePlannerStatistics("t", "ix", 10, options: new[] { "5" }));
        Assert.Throws<FormatException>(() => SqlitePlannerStatistics.Parse("t", "ix", "unordered"));
    }

    private static async Task<string> PlanAsync(SqliteConnection connection, string sql)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = "EXPLAIN QUERY PLAN " + sql;
        await using var reader = await command.ExecuteReaderAsync();
        var details = new List<string>();
        while (await reader.ReadAsync())
        {
            details.Add(reader.GetString(3));
        }

        return string.Join(" / ", details);
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
