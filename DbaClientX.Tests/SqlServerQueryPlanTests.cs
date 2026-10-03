using System.Data;
using System.Xml;
using DBAClientX;
using DBAClientX.QueryBuilder;
using DBAClientX.QueryPlans;

namespace DbaClientX.Tests;

public sealed class SqlServerQueryPlanTests
{
    [Fact]
    public void NativePlan_PreservesNamesFractionalEstimatesAndAccessSemantics()
    {
        var plan = SqlServerQueryPlanParser.Parse("SELECT * FROM Actual p", Document(@"
<RelOp NodeId=""0"" PhysicalOp=""Top"" LogicalOp=""Top"" EstimateRows=""1"">
  <Top><RelOp NodeId=""1"" PhysicalOp=""Clustered Index Scan"" LogicalOp=""Clustered Index Scan""
    EstimateRows=""1000.44"" EstimatedRowsRead=""3000"" TableCardinality=""3000"" EstimatedTotalSubtreeCost=""1.2E-2"">
    <IndexScan><Object Database=""[Db]"" Schema=""[Case]"" Table=""[p]"" Alias=""[Alias]]Name]"" Index=""[IX_p]""/></IndexScan>
  </RelOp></Top>
</RelOp>"), DbaQueryPlanParameterMode.TypedVariables);
        Assert.Equal(SqlDialect.SqlServer, plan.Provenance.Dialect);
        Assert.Equal(DbaQueryPlanParameterMode.TypedVariables, plan.Provenance.ParameterMode);
        Assert.Empty(plan.AmbiguousAliases);
        Assert.Null(plan.Steps[0].Table); // A parent's descendant source is not its own source.
        var scan = Assert.Single(plan.ScanOperations);
        Assert.Equal("p", scan.Table); // Native identity must not be rewritten to Actual.
        Assert.Equal("Case", scan.Schema);
        Assert.Equal("Db", scan.Database);
        Assert.Equal("Alias]Name", scan.Alias);
        Assert.Equal(1000.44, scan.Estimates!.OutputRows);
        Assert.Equal(3000d, scan.Estimates.RowsRead);
        Assert.Equal(3000d, scan.Estimates.TableRows);
        Assert.Equal(.012, scan.Estimates.SubtreeCost);
        Assert.Null(scan.EstimatedRows); // Legacy SQLite estimates retain their own meaning.
        Assert.Null(scan.TableRows);
        Assert.Equal(0, scan.ParentId);
        Assert.Contains(Environment.NewLine + "  Clustered Index Scan", plan.ToString());
        Assert.Throws<NotSupportedException>(() => plan.FullScans.ToArray());
        Assert.Throws<NotSupportedException>(() => QueryPlanAssert.Check(plan, new QueryPlanRules("p")));
    }

    [Fact]
    public void LegacyConstructorsAndSqliteAliasContractsRemainAvailable()
    {
        Assert.NotNull(typeof(DbaQueryPlan).GetConstructor(new[] { typeof(string), typeof(IReadOnlyList<DbaQueryPlanStep>) }));
        Assert.NotNull(typeof(DbaQueryPlanStep).GetConstructor(new[] { typeof(int), typeof(int), typeof(string), typeof(DbaQueryPlanOperation),
            typeof(string), typeof(string), typeof(bool), typeof(string), typeof(string), typeof(bool), typeof(long?), typeof(long?) }));
        var plan = new DbaQueryPlan("SELECT * FROM Actual p", new[] { new DbaQueryPlanStep(1, 0, "SCAN p", DbaQueryPlanOperation.Scan, "p") });
        Assert.Equal("Actual", Assert.Single(plan.FullScans).Table);
        Assert.Single(QueryPlanAssert.Check(plan, new QueryPlanRules("Actual")).Violations);
    }

    [Theory]
    [InlineData("EstimateRows=\"NaN\"")]
    [InlineData("EstimatedRowsRead=\"-1\"")]
    [InlineData("TableCardinality=\"Infinity\"")]
    public void InvalidNativeEstimatesAreRejected(string attribute)
        => Assert.Throws<FormatException>(() => SqlServerQueryPlanParser.Parse("SELECT 1",
            Document("<RelOp NodeId=\"0\" PhysicalOp=\"Constant Scan\" " + attribute + "/>")));

    [Fact]
    public void MissingEstimatesRemainUnknownAndDtdRuntimeAndMultiplePlansAreRejected()
    {
        var plan = SqlServerQueryPlanParser.Parse("SELECT 1", Document("<RelOp NodeId=\"0\" PhysicalOp=\"Constant Scan\"/>"));
        Assert.Null(Assert.Single(plan.Steps).Estimates!.OutputRows);
        Assert.Empty(plan.ScanOperations);
        Assert.Throws<XmlException>(() => SqlServerQueryPlanParser.Parse("SELECT 1", "<!DOCTYPE x [<!ENTITY e SYSTEM 'file:///private'>]>" + Document("&e;")));
        Assert.Throws<FormatException>(() => SqlServerQueryPlanParser.Parse("SELECT 1", Document("<RelOp NodeId=\"0\" PhysicalOp=\"Constant Scan\"><RunTimeCountersPerThread/></RelOp>")));
        Assert.Throws<FormatException>(() => SqlServerQueryPlanParser.Parse("SELECT 1", Document("<RelOp NodeId=\"0\" PhysicalOp=\"Constant Scan\"/>")
            .Replace("</QueryPlan>", "</QueryPlan><QueryPlan/>", StringComparison.Ordinal)));
    }

    [Fact]
    public void SelectWithoutQueryKeepsNativeClassificationAndDoesNotInventOperators()
    {
        var plan = SqlServerQueryPlanParser.Parse("SELECT 1", "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\">"
            + "<BatchSequence><Batch><Statements><StmtSimple StatementType=\"SELECT WITHOUT QUERY\"/></Statements></Batch></BatchSequence></ShowPlanXML>");
        Assert.Empty(plan.Steps);
        Assert.Equal("SELECT WITHOUT QUERY", plan.Provenance.StatementType);
        Assert.Throws<NotSupportedException>(() => QueryPlanAssert.Check(plan, new QueryPlanRules()));
    }

    [Theory]
    [InlineData("SELECT 1; DELETE FROM t")]
    [InlineData("SET SHOWPLAN_XML OFF")]
    [InlineData("SELECT 1 SET SHOWPLAN_XML OFF DELETE FROM t")]
    [InlineData("EXEC('DELETE FROM t')")]
    [InlineData("WITH t AS (SELECT 1 AS x) SELECT x FROM t EXEC('DELETE FROM t')")]
    [InlineData("SELECT @id")]
    public async Task UnsupportedCaptureShapesFailBeforeOpeningAConnection(string query)
    {
        using var provider = new SqlServer();
        provider.ConnectionOptions.ConnectionFactory = _ => throw new Exception("Must not connect");
        await Assert.ThrowsAsync<ArgumentException>(() => provider.ExplainQueryPlanAsync("Server=unavailable;Database=master;Integrated Security=true", query));
    }

    [Theory]
    [InlineData(SqlDbType.NVarChar, null, null, null)]
    [InlineData(SqlDbType.NVarChar, 4001, null, null)]
    [InlineData(SqlDbType.Char, -1, null, null)]
    [InlineData(SqlDbType.Int, 1, null, null)]
    [InlineData(SqlDbType.Decimal, null, (byte)2, (byte)3)]
    [InlineData(SqlDbType.Time, null, null, (byte)8)]
    public void InvalidTypeFacetsAreRejected(SqlDbType type, int? size, byte? precision, byte? scale)
        => Assert.ThrowsAny<ArgumentException>(() => new SqlServerQueryPlanParameter(type, size, precision, scale));

    [Theory]
    [InlineData(SqlDbType.Structured)]
    [InlineData(SqlDbType.Udt)]
    public void UnsupportedParameterTypesAreExplicit(SqlDbType type)
        => Assert.Throws<NotSupportedException>(() => new SqlServerQueryPlanParameter(type));

    [Fact]
    public async Task PreCanceledCapturePreservesTokenAndSkipsFactory()
    {
        using var provider = new SqlServer();
        provider.ConnectionOptions.ConnectionFactory = _ => throw new Exception("Must not connect");
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => provider.ExplainQueryPlanAsync(
            "Server=unavailable;Database=master;Integrated Security=true", "SELECT 1", cancellationToken: cancellation.Token));
        Assert.Equal(cancellation.Token, error.CancellationToken);
    }

    private static string Document(string operators) => "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\">"
        + "<BatchSequence><Batch><Statements><StmtSimple><QueryPlan>" + operators
        + "</QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>";
}
