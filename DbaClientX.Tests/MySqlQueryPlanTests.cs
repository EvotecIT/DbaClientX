using DBAClientX;
using DBAClientX.QueryBuilder;
using DBAClientX.QueryPlans;
using MySqlConnector;

namespace DbaClientX.Tests;

public sealed class MySqlQueryPlanTests
{
    private const string OfflineConnection = "Server=unavailable;Database=plans;User ID=reader;SSL Mode=Required";

    [Fact]
    public void Parse_MySqlPreservesJoinPrefixMeaningAndNativeAliasWithoutSqlInference()
    {
        var plan = MySqlQueryPlanParser.Parse("SELECT * FROM Wrong AS NativeAlias", """
            {"query_block":{"select_id":1,"cost_info":{"query_cost":"9.25"},"nested_loop":[
              {"table":{"table_name":"NativeAlias","access_type":"ALL","rows_examined_per_scan":17.5,
                "rows_produced_per_join":12,"cost_info":{"prefix_cost":"8.5","read_cost":"2.1"}}},
              {"table":{"table_name":"<derived2>","access_type":"ref","key":"KeyCase","rows_examined_per_scan":2,
                "materialized_from_subquery":{"query_block":{"select_id":2,"table":{"table_name":"Inner","access_type":"index"}}}}}
            ]}}
            """, MySqlQueryPlanFormat.MySqlJsonV1, DbaQueryPlanParameterMode.BoundValues);
        Assert.Equal(SqlDialect.MySql, plan.Provenance.Dialect);
        Assert.Equal("MySQL EXPLAIN JSON v1", plan.Provenance.Format);
        Assert.Equal(DbaQueryPlanParameterMode.BoundValues, plan.Provenance.ParameterMode);
        Assert.Equal(9.25, plan.Steps[0].Estimates!.SubtreeCost);
        Assert.Equal(new[] { -1, 0, 1, 1, 3, 4, 5 }, plan.Steps.Select(step => step.ParentId));
        Assert.Equal(Enumerable.Range(0, 7), plan.Steps.Select(step => step.Id));
        var scan = plan.Steps[2];
        Assert.Equal("NativeAlias", scan.Table);
        Assert.Null(scan.Alias);
        Assert.Null(scan.Schema);
        Assert.Null(scan.Database);
        Assert.Equal(17.5, scan.Estimates!.RowsRead);
        Assert.Null(scan.Estimates.OutputRows);
        Assert.Null(scan.Estimates.TableRows);
        Assert.Null(scan.Estimates.SubtreeCost);
        Assert.Contains("rows_produced_per_join=12", scan.Detail);
        Assert.Contains("prefix_cost=8.5", scan.Detail);
        Assert.Equal(DbaQueryPlanOperation.Search, plan.Steps[3].Operation);
        Assert.Equal("KeyCase", plan.Steps[3].Index);
        Assert.Throws<NotSupportedException>(() => plan.FullScans.ToArray());
        Assert.Throws<NotSupportedException>(() => QueryPlanAssert.Check(plan, new QueryPlanRules("Wrong")));
    }

    [Fact]
    public void Parse_MariaDbDoesNotMultiplyFilteredOrInferUnknownEstimates()
    {
        var plan = MySqlQueryPlanParser.Parse("SELECT * FROM t", """
            {"query_block":{"select_id":1,"read_sorted_file":{"filesort":{"sort_key":"a.id","table":
              {"table_name":"a","access_type":"ALL","rows":11,"filtered":20}}}}}
            """, MySqlQueryPlanFormat.MariaDbJson);
        Assert.Equal("MariaDB EXPLAIN JSON", plan.Provenance.Format);
        var scan = Assert.Single(plan.ScanOperations);
        Assert.Equal(11, scan.Estimates!.RowsRead);
        Assert.Null(scan.Estimates.OutputRows);
        Assert.Null(scan.Estimates.SubtreeCost);
        Assert.Null(scan.EstimatedRows);
        Assert.Null(scan.TableRows);
    }

    [Theory]
    [InlineData("{\"query_block\":{\"select_id\":1,\"select_id\":2}}")]
    [InlineData("{\"query_block\":{\"r_total_time_ms\":0}}")]
    [InlineData("{\"query_plan\":{\"operation\":\"Table scan\"}}")]
    [InlineData("[{\"query_block\":{}}]")]
    [InlineData("{\"query_block\":{\"table\":{\"rows_examined_per_scan\":-1}}}")]
    [InlineData("{\"query_block\":{\"table\":{\"rows_examined_per_scan\":\"NaN\"}}}")]
    [InlineData("{\"query_block\":{\"nested_loop\":1}}")]
    [InlineData("{\"query_block\":{\"table\":{\"key\":17}}}")]
    public void Parse_RejectsAmbiguousRuntimeUnsupportedOrMalformedDocuments(string json)
        => Assert.Throws<FormatException>(() => MySqlQueryPlanParser.Parse("SELECT 1", json, MySqlQueryPlanFormat.MySqlJsonV1));

    [Fact]
    public void Parse_RejectsWrongSchemaAndBoundedResources()
    {
        Assert.Throws<FormatException>(() => MySqlQueryPlanParser.Parse("SELECT 1",
            "{\"query_block\":{\"table\":{\"rows\":1}}}", MySqlQueryPlanFormat.MySqlJsonV1));
        Assert.Throws<FormatException>(() => MySqlQueryPlanParser.Parse("SELECT 1",
            new string(' ', 16 * 1024 * 1024 + 1), MySqlQueryPlanFormat.MariaDbJson));
        var wide = "{\"query_block\":{\"nested_loop\":[" + string.Join(",", Enumerable.Repeat("{\"table\":{}}", 4096)) + "]}}";
        Assert.Throws<FormatException>(() => MySqlQueryPlanParser.Parse("SELECT 1", wide, MySqlQueryPlanFormat.MySqlJsonV1));
        var deep = string.Concat(Enumerable.Repeat("{\"query_block\":", 129)) + "{}" + new string('}', 129);
        Assert.Throws<FormatException>(() => MySqlQueryPlanParser.Parse("SELECT 1", deep, MySqlQueryPlanFormat.MySqlJsonV1));
    }

    [Theory]
    [InlineData("SELECT 1; UPDATE t SET id=2")]
    [InlineData("SELECT [x; UPDATE t SET id=2]")]
    [InlineData("SELECT 1 /*! ; UPDATE t SET id=2 */")]
    [InlineData("SELECT 1 /*M! ; UPDATE t SET id=2 */")]
    [InlineData("EXPLAIN SELECT 1")]
    [InlineData("ANALYZE SELECT 1")]
    [InlineData("DROP TABLE t")]
    [InlineData("UPDATE t SET id=2")]
    [InlineData("INSERT INTO t VALUES(1)")]
    [InlineData("DELETE FROM t")]
    [InlineData("REPLACE INTO t VALUES(1)")]
    [InlineData("SELECT ?")]
    [InlineData("SELECT :value")]
    [InlineData("SELECT @missing")]
    public async Task Capture_InvalidInputNeverCreatesConnection(string sql)
    {
        var client = new InspectingClient();
        await Assert.ThrowsAsync<ArgumentException>(() => client.ExplainQueryPlanAsync(OfflineConnection, sql));
        Assert.Equal(0, client.Created);
    }

    [Fact]
    public async Task Capture_ValidLexicalBoundariesUseIsolatedSettingsAndDisposeOnFailure()
    {
        var client = new InspectingClient();
        await Assert.ThrowsAsync<DbaQueryExecutionException>(() => client.ExplainQueryPlanAsync(OfflineConnection,
            "; /* ordinary comment */ SELECT 'a;\\\'b', \"a;\\\"b\", `a;b`, @@SESSION.sql_mode, @value; #comment",
            new Dictionary<string, object?> { ["?value"] = null }, new Dictionary<string, MySqlDbType> { ["@value"] = MySqlDbType.Int32 }));
        Assert.Equal(1, client.Created);
        Assert.Equal(1, client.Disposed);
        Assert.False(client.Settings!.Pooling);
        Assert.False(client.Settings.AutoEnlist);
        Assert.False(client.Settings.AllowUserVariables);
        Assert.Equal(2, client.Settings.CancellationTimeout);
        Assert.Equal(5u, client.Settings.DefaultCommandTimeout);
    }

    [Fact]
    public async Task Capture_PreCancellationPreservesTokenAndAvoidsFactory()
    {
        var client = new InspectingClient();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.ExplainQueryPlanAsync(
            OfflineConnection, "SELECT 1", cancellationToken: cancellation.Token));
        Assert.Equal(cancellation.Token, error.CancellationToken);
        Assert.Equal(0, client.Created);
    }

    [Theory]
    [InlineData("SSL Mode=None")]
    [InlineData("Pooling=true")]
    [InlineData("Auto Enlist=true")]
    [InlineData("Allow User Variables=true")]
    [InlineData("Cancellation Timeout=0")]
    [InlineData("Default Command Timeout=0")]
    public async Task Capture_RejectsUnsafeFactoryAndStillDisposes(string overrideSetting)
    {
        var client = new InspectingClient(overrideSetting);
        await Assert.ThrowsAsync<DbaQueryExecutionException>(() => client.ExplainQueryPlanAsync(OfflineConnection, "SELECT 1"));
        Assert.Equal(0, client.Opened);
        Assert.Equal(1, client.Disposed);
    }

    private sealed class InspectingClient(string? overrideSetting = null) : MySql
    {
        public int Created, Opened, Disposed;
        public MySqlConnectionStringBuilder? Settings;
        protected override MySqlConnection CreateConnection(string connectionString)
        {
            Created++;
            Settings = new MySqlConnectionStringBuilder(connectionString);
            return new MySqlConnection(connectionString + (overrideSetting == null ? "" : ";" + overrideSetting));
        }
        protected override Task OpenConnectionAsync(MySqlConnection connection, CancellationToken token)
        { Opened++; throw new InvalidOperationException("Observed isolated startup."); }
        protected override ValueTask DisposeConnectionAsync(MySqlConnection connection)
        { Disposed++; return base.DisposeConnectionAsync(connection); }
    }
}
