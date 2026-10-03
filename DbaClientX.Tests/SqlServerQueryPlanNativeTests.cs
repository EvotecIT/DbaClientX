using System.Data;
using DBAClientX;
using DBAClientX.QueryPlans;
using Microsoft.Data.SqlClient;

namespace DbaClientX.Tests;

public sealed class SqlServerQueryPlanNativeTests
{
    [Fact]
    public async Task Capture_PreservesNativeEstimateMeaningAndNeverExecutesExplainedDml()
    {
        await using var fixture = await SqlServerPlanTestScope.CreateAsync();
        using var provider = new TrackingProvider();
        var seek = await provider.ExplainQueryPlanAsync(fixture.Connection, "SELECT Payload FROM dbo.Events WHERE Id=42");
        Assert.Contains(seek.Steps, step => step.Operation == DbaQueryPlanOperation.Search && step.Table == "Events" && step.Schema == "dbo");
        var filtered = await provider.ExplainQueryPlanAsync(fixture.Connection, "SELECT Payload FROM dbo.Events WITH(INDEX(PK_Events)) WHERE Payload>1");
        var scan = Assert.Single(filtered.ScanOperations);
        Assert.Equal(3000d, scan.Estimates!.TableRows);
        Assert.Equal(3000d, scan.Estimates.RowsRead);
        Assert.True(scan.Estimates.OutputRows > 0 && scan.Estimates.OutputRows < scan.Estimates.RowsRead);
        var top = provider.ExplainQueryPlan(fixture.Connection, "SELECT TOP(1) Id FROM dbo.Events ORDER BY Id");
        Assert.Contains(top.ScanOperations, step => step.Estimates!.RowsRead == 1 && step.Estimates.TableRows == 3000);
        var max = await provider.ExplainQueryPlanAsync(fixture.Connection, "SELECT MAX(Id) FROM dbo.Events");
        Assert.Contains(max.ScanOperations, step => step.Estimates!.RowsRead == 1);
        var typed = await provider.ExplainQueryPlanAsync(fixture.Connection, "DELETE FROM dbo.Events WHERE Id=@id",
            new Dictionary<string, SqlServerQueryPlanParameter> { ["@id"] = new(SqlDbType.Int) });
        Assert.Equal(DbaQueryPlanParameterMode.TypedVariables, typed.Provenance.ParameterMode);
        Assert.Equal(3000, await fixture.ScalarAsync("SELECT COUNT(*) FROM dbo.Events"));
        var literal = await provider.ExplainQueryPlanAsync(fixture.Connection,
            "SELECT '@unused' AS [SHOWPLAN_XML] FROM dbo.Events e WHERE e.Id=1 /* outer /* nested */ end */");
        Assert.Equal(DbaQueryPlanParameterMode.None, literal.Provenance.ParameterMode);
        Assert.All(provider.Connections, connection => Assert.Equal(ConnectionState.Closed, connection.State));
    }

    [Fact]
    public async Task Capture_TypeFacetsAndActiveTransactionRemainIsolated()
    {
        await using var fixture = await SqlServerPlanTestScope.CreateAsync(caseSensitive: true);
        using var provider = new TrackingProvider();
        await provider.BeginTransactionAsync(fixture.Connection);
        try
        {
            provider.ExecuteNonQuery(fixture.Connection, "INSERT dbo.Events VALUES(4001,1)", useTransaction: true);
            using var ambient = new System.Transactions.TransactionScope(System.Transactions.TransactionScopeAsyncFlowOption.Enabled);
            var plan = await provider.ExplainQueryPlanAsync(fixture.Connection,
                "SELECT @n, @d, @t, @b, @x, @@VERSION FROM dbo.Events WHERE Id=1", new Dictionary<string, SqlServerQueryPlanParameter>
                {
                    ["@n"] = new(SqlDbType.NVarChar, -1), ["@d"] = new(SqlDbType.Decimal, precision: 17, scale: 5),
                    ["@t"] = new(SqlDbType.DateTime2, scale: 3), ["@b"] = new(SqlDbType.VarBinary, 16), ["@x"] = new(SqlDbType.Xml)
                });
            Assert.Equal(DbaQueryPlanParameterMode.TypedVariables, plan.Provenance.ParameterMode);
            Assert.NotEmpty(plan.Steps);
            var constant = await provider.ExplainQueryPlanAsync(fixture.Connection, "SELECT @@VERSION");
            Assert.Equal("SELECT WITHOUT QUERY", constant.Provenance.StatementType);
            Assert.Empty(constant.Steps);
            var aliases = await provider.ExplainQueryPlanAsync(fixture.Connection,
                "SELECT e.Id,E.Id FROM dbo.Events e JOIN dbo.Events E ON e.Id=E.Id+1 WHERE e.Id=10");
            Assert.Contains(aliases.Steps, step => step.Alias == "e");
            Assert.Contains(aliases.Steps, step => step.Alias == "E");
            Assert.True(provider.IsInTransaction);
            Assert.Equal(3001, Convert.ToInt32(provider.ExecuteScalar(fixture.Connection, "SELECT COUNT(*) FROM dbo.Events", useTransaction: true)));
            ambient.Complete();
        }
        finally { provider.Rollback(); }
        Assert.Equal(3000, await fixture.ScalarAsync("SELECT COUNT(*) FROM dbo.Events"));
        Assert.All(provider.Connections, connection => Assert.Equal(ConnectionState.Closed, connection.State));
    }

    [Fact]
    public async Task Capture_PermissionFailureClosesSessionAndSubsequentCaptureWorks()
    {
        await using var fixture = await SqlServerPlanTestScope.CreateAsync();
        await fixture.ExecuteAsync("CREATE USER PlanReader WITHOUT LOGIN; GRANT SELECT ON dbo.Events TO PlanReader;");
        using var denied = new TrackingProvider("PlanReader");
        var failure = await Assert.ThrowsAsync<DbaQueryExecutionException>(() => denied.ExplainQueryPlanAsync(fixture.Connection, "SELECT * FROM dbo.Events"));
        Assert.Equal(262, failure.ProviderErrorCode); // SHOWPLAN permission denied.
        Assert.All(denied.Connections, connection => Assert.Equal(ConnectionState.Closed, connection.State));
        using var allowed = new SqlServer();
        Assert.NotEmpty((await allowed.ExplainQueryPlanAsync(fixture.Connection, "SELECT * FROM dbo.Events")).Steps);
    }

    [Fact]
    public async Task Capture_RunningCancellationAndTimeoutCloseTheirOwnedSessionsWithoutReplay()
    {
        await using var fixture = await SqlServerPlanTestScope.CreateAsync();
        await using var blocker = new SqlConnection(fixture.Connection);
        await blocker.OpenAsync();
        using var transaction = blocker.BeginTransaction();
        using (var lockCommand = new SqlCommand("ALTER TABLE dbo.Events ADD Held int NULL", blocker, transaction))
            await lockCommand.ExecuteNonQueryAsync();
        try
        {
            using var provider = new TrackingProvider { CommandTimeout = 30, MaxRetryAttempts = 5 };
            using var cancellation = new CancellationTokenSource();
            var capture = provider.ExplainQueryPlanAsync(fixture.Connection, "SELECT Payload FROM dbo.Events", cancellationToken: cancellation.Token);
            int session = await provider.Opened.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await fixture.WaitForSchemaBlockAsync(session);
            cancellation.Cancel();
            var failure = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => capture.WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.Equal(cancellation.Token, failure.CancellationToken);
            Assert.Single(provider.Connections);
            Assert.Equal(ConnectionState.Closed, provider.Connections[0].State);

            using var timeout = new TrackingProvider { CommandTimeout = 1, MaxRetryAttempts = 5 };
            var deadline = await Assert.ThrowsAsync<DbaQueryExecutionException>(() => timeout.ExplainQueryPlanAsync(fixture.Connection, "SELECT Payload FROM dbo.Events"));
            Assert.Equal(-2, deadline.ProviderErrorCode);
            Assert.Single(timeout.Connections);
            Assert.Equal(ConnectionState.Closed, timeout.Connections[0].State);
        }
        finally { transaction.Rollback(); }
        Assert.Equal(3000, await fixture.ScalarAsync("SELECT COUNT(*) FROM dbo.Events"));
    }

    private sealed class TrackingProvider(string? user = null) : SqlServer
    {
        internal List<SqlConnection> Connections { get; } = new();
        internal TaskCompletionSource<int> Opened { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        protected override SqlConnection CreateConnection(string connectionString)
        {
            var connection = base.CreateConnection(connectionString);
            Connections.Add(connection);
            return connection;
        }
        protected override async Task OpenConnectionAsync(SqlConnection connection, CancellationToken cancellationToken)
        {
            await base.OpenConnectionAsync(connection, cancellationToken);
            if (user != null)
            {
                using var impersonate = new SqlCommand("EXECUTE AS USER = '" + user + "'", connection);
                await impersonate.ExecuteNonQueryAsync(cancellationToken);
            }
            Opened.TrySetResult(connection.ServerProcessId);
        }
    }
}
