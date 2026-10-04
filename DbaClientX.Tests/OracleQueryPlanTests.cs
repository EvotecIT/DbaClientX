using System.Data;
using DBAClientX;
using DBAClientX.QueryBuilder;
using DBAClientX.QueryPlans;
using Oracle.ManagedDataAccess.Client;

namespace DbaClientX.Tests;

public sealed class OracleQueryPlanTests
{
    private const string Offline = "Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=unavailable)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=plans)));User Id=reader;Password=unused";

    [Fact]
    public void Import_PreservesNativeIdentityAndPreciseNullableEstimatesWithoutInferringTableOrDatabase()
    {
        var rows = Rows();
        rows.Rows.Add(4294967295m, 10m, DBNull.Value, "SELECT STATEMENT", DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, 1.75m, 8.25m);
        rows.Rows.Add(4294967295m, 21m, 10m, "INDEX", "RANGE SCAN", "Owner.Case", "Index.Case", "INDEX", "NativeAlias@SEL$1", DBNull.Value, 2.5m);
        var plan = OracleQueryPlanParser.Parse("SELECT * FROM guessed", rows, DbaQueryPlanParameterMode.BoundValues);
        Assert.Equal(SqlDialect.Oracle, plan.Provenance.Dialect);
        Assert.Equal("PLAN_TABLE", plan.Provenance.Format);
        Assert.Equal(DbaQueryPlanParameterMode.BoundValues, plan.Provenance.ParameterMode);
        Assert.Equal(1.75, plan.Steps[0].Estimates!.OutputRows);
        Assert.Equal(8.25, plan.Steps[0].Estimates!.SubtreeCost);
        var index = plan.Steps[1];
        Assert.Equal(21, index.Id); Assert.Equal(10, index.ParentId);
        Assert.Equal("Index.Case", index.Index); Assert.Null(index.Table);
        Assert.Equal("Owner.Case", index.Schema); Assert.Equal("NativeAlias@SEL$1", index.Alias);
        Assert.Null(index.Estimates!.OutputRows); Assert.Null(index.Estimates.RowsRead); Assert.Null(index.Estimates.TableRows); Assert.Null(index.Database);
        Assert.Throws<NotSupportedException>(() => plan.FullScans);
    }

    [Theory]
    [InlineData("duplicate")]
    [InlineData("parent")]
    [InlineData("cycle")]
    [InlineData("mixed")]
    [InlineData("fraction")]
    [InlineData("negative")]
    [InlineData("infinite")]
    [InlineData("text")]
    public void Import_RejectsMalformedNativeTreeAndEstimates(string corruption)
    {
        var rows = Rows();
        rows.Rows.Add(1m, 0m, DBNull.Value, "SELECT STATEMENT", DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, 1m, 2m);
        rows.Rows.Add(1m, 1m, 0m, "TABLE ACCESS", "FULL", "OWNER", "TABLE", "TABLE", DBNull.Value, 1m, 2m);
        switch (corruption)
        {
            case "duplicate": rows.Rows[1]["ID"] = 0m; break;
            case "parent": rows.Rows[1]["PARENT_ID"] = 9m; break;
            case "cycle": rows.Rows[1]["PARENT_ID"] = 1m; break;
            case "mixed": rows.Rows[1]["PLAN_ID"] = 2m; break;
            case "fraction": rows.Rows[1]["ID"] = 2147483646.999999999m; break;
            case "negative": rows.Rows[1]["CARDINALITY"] = -1m; break;
            case "infinite": rows.Rows[1]["COST"] = double.PositiveInfinity; break;
            case "text": rows.Rows[1]["OBJECT_NAME"] = new string('x', 4097); break;
        }
        Assert.Throws<FormatException>(() => OracleQueryPlanParser.Parse("SELECT 1", rows));
    }

    [Fact]
    public void Import_RejectsUnboundedDepthAndOperatorCount()
    {
        foreach (int count in new[] { 129, 4097 })
        {
            var rows = Rows();
            for (int id = 0; id < count; id++) rows.Rows.Add(1m, id, id == 0 ? DBNull.Value : id - 1,
                "OP", DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value);
            Assert.Throws<FormatException>(() => OracleQueryPlanParser.Parse("SELECT 1", rows));
        }
    }

    [Theory]
    [InlineData("DELETE FROM Events")]
    [InlineData("WITH x AS (SELECT 1 FROM dual) DELETE FROM Events")]
    [InlineData("WITH FUNCTION f RETURN NUMBER IS BEGIN RETURN 1; END; SELECT f FROM dual")]
    [InlineData("SELECT 1 FROM dual; DELETE FROM Events")]
    [InlineData("EXPLAIN PLAN FOR SELECT 1 FROM dual")]
    [InlineData("SELECT :missing FROM dual")]
    [InlineData("SELECT ? FROM dual")]
    public async Task Capture_RejectsUnsupportedStatementOrBindingBeforeOpening(string sql)
    {
        using var client = new UnopenedClient();
        await Assert.ThrowsAsync<ArgumentException>(() => client.ExplainQueryPlanAsync(Offline, sql));
        Assert.Equal(0, client.Opened);
    }

    [Fact]
    public async Task Capture_TokenizesAlternativeQuotesAndSnapshotsNativeBindingsBeforeAwaiting()
    {
        using var client = new UnopenedClient();
        var values = new Dictionary<string, object?> { [":tenant$Id#"] = 42 };
        var types = new Dictionary<string, OracleDbType> { ["TENANT$id#"] = OracleDbType.Int32 };
        var capture = client.ExplainQueryPlanAsync(Offline,
            "WITH x AS (SELECT :tenant$Id# AS id, nq'[; :ignored]' AS s FROM dual) SELECT d.id FROM x d", values, types);
        values.Clear(); types.Clear();
        await Assert.ThrowsAsync<DbaQueryExecutionException>(() => capture);
        Assert.Equal(1, client.Opened);
        var isolated = new OracleConnectionStringBuilder(client.Captured!.ConnectionString);
        Assert.False(isolated.Pooling); Assert.Equal("false", isolated.Enlist);
        Assert.True(client.Captured.SSLServerDNMatch);
    }

    [Theory]
    [InlineData("localhost/plans")]
    [InlineData("(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521)))")]
    [InlineData("(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=localhost))(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)))")]
    [InlineData("(DESCRIPTION=(HOST=\"(PROTOCOL=TCPS)\"))")]
    public async Task Capture_RejectsPlaintextFallbackOrUnresolvedTransport(string source)
    {
        using var client = new UnopenedClient();
        var builder = new OracleConnectionStringBuilder(Offline) { DataSource = source };
        await Assert.ThrowsAsync<ArgumentException>(() => client.ExplainQueryPlanAsync(builder.ConnectionString, "SELECT 1 FROM dual"));
        Assert.Equal(0, client.Opened);
    }

    private static DataTable Rows()
    {
        var table = new DataTable();
        foreach (string name in new[] { "PLAN_ID", "ID", "PARENT_ID", "OPERATION", "OPTIONS", "OBJECT_OWNER", "OBJECT_NAME", "OBJECT_TYPE", "OBJECT_ALIAS", "CARDINALITY", "COST" })
            table.Columns.Add(name, typeof(object));
        return table;
    }

    private sealed class OpenSentinel : Exception { }
    private sealed class UnopenedClient : DBAClientX.Oracle
    {
        internal int Opened; internal OracleConnection? Captured;
        protected override Task OpenConnectionAsync(OracleConnection connection, CancellationToken token)
        { Opened++; Captured = connection; throw new OpenSentinel(); }
    }
}
