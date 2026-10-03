using System.Data;
using System.Text.Json;
using DBAClientX;
using DBAClientX.QueryBuilder;
using DBAClientX.QueryPlans;
using Npgsql;
using NpgsqlTypes;

namespace DbaClientX.Tests;

public sealed class PostgreSqlQueryPlanTests
{
    private const string OfflineConnection = "Host=unavailable;Database=plans;Username=reader;SSL Mode=Require";

    [Fact]
    public void Parse_PreservesNativeIdentityEstimateMeaningAndPreorderParentage()
    {
        var plan = PostgreSqlQueryPlanParser.Parse("SELECT * FROM Wrong e", """
            [{"Plan":{"Node Type":"Limit","Plan Rows":1,"Total Cost":0.012,"Plans":[
              {"Node Type":"Seq Scan","Relation Name":"e","Schema":"Case","Alias":"E","Plan Rows":17.25,"Total Cost":2.5},
              {"Node Type":"Index Scan","Relation Name":"Events","Index Name":"PK_Events","Alias":"e","Index Cond":"(id = 1)","Plan Rows":1}
            ]}}]
            """, DbaQueryPlanParameterMode.BoundValues);
        Assert.Equal(SqlDialect.PostgreSql, plan.Provenance.Dialect);
        Assert.Equal("EXPLAIN JSON", plan.Provenance.Format);
        Assert.Equal(DbaQueryPlanParameterMode.BoundValues, plan.Provenance.ParameterMode);
        Assert.Null(plan.Provenance.StatementType);
        Assert.Equal(new[] { 0, 1, 2 }, plan.Steps.Select(step => step.Id));
        Assert.Equal(new[] { -1, 0, 0 }, plan.Steps.Select(step => step.ParentId));
        Assert.Null(plan.Steps[0].Table);
        var scan = Assert.Single(plan.ScanOperations);
        Assert.Equal("e", scan.Table);
        Assert.Equal("E", scan.Alias);
        Assert.Equal("Case", scan.Schema);
        Assert.Null(scan.Database);
        Assert.Equal(17.25, scan.Estimates!.OutputRows);
        Assert.Equal(2.5, scan.Estimates.SubtreeCost);
        Assert.Null(scan.Estimates.RowsRead);
        Assert.Null(scan.Estimates.TableRows);
        Assert.Null(scan.EstimatedRows);
        Assert.Null(scan.TableRows);
        Assert.Equal(DbaQueryPlanOperation.Search, plan.Steps[2].Operation);
        Assert.Equal("PK_Events", plan.Steps[2].Index);
        Assert.Empty(plan.AmbiguousAliases);
        Assert.Throws<NotSupportedException>(() => plan.FullScans.ToArray());
        Assert.Throws<NotSupportedException>(() => QueryPlanAssert.Check(plan, new QueryPlanRules("Events")));
        Assert.Contains(Environment.NewLine + "  Seq Scan", plan.ToString());
    }

    [Fact]
    public void Parse_UnknownOperatorsAndMissingEstimatesRemainUnknown()
    {
        var plan = PostgreSqlQueryPlanParser.Parse("SELECT 1", """[{"Plan":{"Node Type":"Future Operator"}}]""");
        var step = Assert.Single(plan.Steps);
        Assert.Equal(DbaQueryPlanOperation.Other, step.Operation);
        Assert.Null(step.Estimates!.OutputRows);
        Assert.Null(step.Estimates.SubtreeCost);
        Assert.Empty(plan.ScanOperations);
        var index = PostgreSqlQueryPlanParser.Parse("SELECT id FROM t ORDER BY id",
            """[{"Plan":{"Node Type":"Index Only Scan","Relation Name":"t","Index Name":"pk"}}]""");
        Assert.Single(index.ScanOperations);
        Assert.False(index.Steps[0].IsCoveringIndex); // SQLite coverage rules are not inferred from the native label.
    }

    [Theory]
    [InlineData("[]")]
    [InlineData("[{},{}]")]
    [InlineData("[{\"Plan\":null}]")]
    [InlineData("[{\"Plan\":{\"Node Type\":\"Result\",\"Plans\":{}}}]")]
    [InlineData("[{\"Plan\":{\"Node Type\":\"Result\",\"Plan Rows\":-1}}]")]
    [InlineData("[{\"Plan\":{\"Node Type\":\"Result\",\"Plan Rows\":1e999}}]")]
    [InlineData("[{\"Plan\":{\"Node Type\":\"Result\",\"Total Cost\":\"1\"}}]")]
    [InlineData("[{\"Plan\":{\"Node Type\":\"Result\",\"Node Type\":\"Seq Scan\"}}]")]
    [InlineData("[{\"Plan\":{\"Node Type\":\"Result\",\"Actual Rows\":1}}]")]
    [InlineData("[{\"Plan\":{\"Node Type\":\"Result\"},\"Execution Time\":1}]")]
    public void Parse_RejectsAmbiguousMalformedAndActualDocuments(string json)
        => Assert.Throws<FormatException>(() => PostgreSqlQueryPlanParser.Parse("SELECT 1", json));

    [Fact]
    public void Parse_EnforcesDocumentDepthAndOperatorBounds()
    {
        Assert.Throws<FormatException>(() => PostgreSqlQueryPlanParser.Parse("SELECT 1", new string(' ', 16 * 1024 * 1024 + 1)));
        var child = "{\"Node Type\":\"Result\"}";
        string tooMany = "[{\"Plan\":{\"Node Type\":\"Append\",\"Plans\":[" + string.Join(",", Enumerable.Repeat(child, 4096)) + "]}}]";
        Assert.Throws<FormatException>(() => PostgreSqlQueryPlanParser.Parse("SELECT 1", tooMany));
        string tooDeep = "[{\"Plan\":{\"Node Type\":\"Result\",\"Extra\":" + new string('[', 130) + "0" + new string(']', 130) + "}}]";
        Assert.Throws<FormatException>(() => PostgreSqlQueryPlanParser.Parse("SELECT 1", tooDeep));
    }

    [Theory]
    [InlineData("SELECT 1; DELETE FROM t")]
    [InlineData("SELECT (ARRAY[']'])[1]; DELETE FROM t")]
    [InlineData("EXPLAIN ANALYZE DELETE FROM t")]
    [InlineData("SET TRANSACTION READ WRITE")]
    [InlineData("SELECT $1")]
    [InlineData("SELECT @missing")]
    public async Task Capture_RejectsUnsupportedInputBeforeConnectionCreation(string query)
    {
        using var provider = new NoConnectionProvider();
        await Assert.ThrowsAsync<ArgumentException>(() => provider.ExplainQueryPlanAsync(OfflineConnection, query));
        Assert.Equal(0, provider.FactoryCalls);
    }

    [Fact]
    public async Task Capture_RejectsAmbiguousUnusedAndMissingParameterContractsBeforeConnecting()
    {
        using var provider = new NoConnectionProvider();
        await Assert.ThrowsAsync<ArgumentException>(() => provider.ExplainQueryPlanAsync(OfflineConnection, "SELECT @id",
            new Dictionary<string, object?> { ["@id"] = 1, ["id"] = 2 }));
        await Assert.ThrowsAsync<ArgumentException>(() => provider.ExplainQueryPlanAsync(OfflineConnection, "SELECT 1",
            new Dictionary<string, object?> { ["id"] = 1 }));
        await Assert.ThrowsAsync<ArgumentException>(() => provider.ExplainQueryPlanAsync(OfflineConnection, "SELECT @id",
            new Dictionary<string, object?> { ["id"] = null }, new Dictionary<string, NpgsqlDbType> { ["other"] = NpgsqlDbType.Integer }));
        Assert.Equal(0, provider.FactoryCalls);
    }

    [Fact]
    public async Task Capture_PreCancellationAndOpeningCleanupPreserveFailureContracts()
    {
        using var provider = new NoConnectionProvider();
        using var cancel = new CancellationTokenSource();
        cancel.Cancel();
        var cancellation = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => provider.ExplainQueryPlanAsync(
            OfflineConnection, "SELECT 1", cancellationToken: cancel.Token));
        Assert.Equal(cancel.Token, cancellation.CancellationToken);
        Assert.Equal(0, provider.FactoryCalls);
        using var failing = new OpeningCleanupFailureProvider();
        var failure = await Assert.ThrowsAsync<AggregateException>(() => failing.ExplainQueryPlanAsync(OfflineConnection, "SELECT 1"));
        Assert.Equal(2, failure.InnerExceptions.Count);
        Assert.All(failure.InnerExceptions, error => Assert.IsType<DbaQueryExecutionException>(error));
        Assert.DoesNotContain("private-sentinel", failure.ToString());
        Assert.True(failing.Disposed);
    }

    private sealed class NoConnectionProvider : PostgreSql
    {
        public int FactoryCalls { get; private set; }
        protected override NpgsqlConnection CreateConnection(string connectionString)
        {
            FactoryCalls++;
            throw new InvalidOperationException("Must not connect");
        }
    }

    private sealed class OpeningCleanupFailureProvider : PostgreSql
    {
        public bool Disposed { get; private set; }
        protected override Task OpenConnectionAsync(NpgsqlConnection connection, CancellationToken token)
            => throw new InvalidOperationException("private-sentinel opening");
        protected override async ValueTask DisposeConnectionAsync(NpgsqlConnection connection)
        {
            await base.DisposeConnectionAsync(connection);
            Disposed = true;
            throw new InvalidOperationException("private-sentinel cleanup");
        }
    }
}
