using System.Data;
using System.Diagnostics;
using DBAClientX;
using DBAClientX.Diagnostics;
using DBAClientX.QueryPlans;
using Npgsql;
using NpgsqlTypes;

namespace DbaClientX.Tests;

public sealed class PostgreSqlQueryPlanNativeTests
{
    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task Capture_PreservesNativeIdentityBindsValuesAndNeverExecutesDml()
    {
        await using var fixture = await PostgreSqlPlanTestScope.CreateAsync();
        using var provider = new TrackingProvider();
        var query = $"SELECT e.\"Id\" FROM {fixture.Table} e WHERE e.\"Payload\"=@payload";
        var rare = await provider.ExplainQueryPlanAsync(fixture.Connection, query, new Dictionary<string, object?> { ["@payload"] = 9 });
        var common = await provider.ExplainQueryPlanAsync(fixture.Connection, query, new Dictionary<string, object?> { ["payload"] = 1 });
        var scan = Assert.Single(rare.ScanOperations);
        Assert.Equal("Events", scan.Table);
        Assert.Equal(fixture.Schema, scan.Schema);
        Assert.Equal("e", scan.Alias);
        Assert.Equal(1d, scan.Estimates!.OutputRows);
        Assert.Equal(2999d, Assert.Single(common.ScanOperations).Estimates!.OutputRows);
        Assert.Null(scan.Estimates.RowsRead);
        Assert.Null(scan.Estimates.TableRows);
        Assert.Equal(DbaQueryPlanParameterMode.BoundValues, rare.Provenance.ParameterMode);
        Assert.Throws<NotSupportedException>(() => rare.FullScans.ToArray());
        var seek = provider.ExplainQueryPlan(fixture.Connection, $"SELECT \"Id\" FROM {fixture.Table} WHERE \"Id\"=:id",
            new Dictionary<string, object?> { ["id"] = 42 });
        Assert.Contains(seek.Steps, step => step.Operation == DbaQueryPlanOperation.Search && step.Index == "Events_pkey");
        var typed = await provider.ExplainQueryPlanAsync(fixture.Connection,
            $"SELECT \"Id\" FROM {fixture.Table} WHERE \"Id\"=ANY(@ids) OR @empty IS NOT NULL",
            new Dictionary<string, object?> { ["ids"] = new[] { 1, 2 }, ["empty"] = null },
            new Dictionary<string, NpgsqlDbType> { ["ids"] = NpgsqlDbType.Array | NpgsqlDbType.Integer, ["empty"] = NpgsqlDbType.Integer });
        Assert.NotEmpty(typed.Steps);
        foreach (var dml in new[]
        {
            $"DELETE FROM {fixture.Table} WHERE \"Id\"=1",
            $"UPDATE {fixture.Table} SET \"Payload\"=99 WHERE \"Id\"=1",
            $"INSERT INTO {fixture.Table} VALUES (4001,99)",
            $"WITH removed AS (DELETE FROM {fixture.Table} RETURNING *) SELECT * FROM removed"
        }) Assert.NotEmpty((await provider.ExplainQueryPlanAsync(fixture.Connection, dml)).Steps);
        Assert.Equal(3000L, await fixture.ScalarAsync($"SELECT count(*) FROM {fixture.Table}"));
        Assert.Equal(1, await fixture.ScalarAsync($"SELECT \"Payload\" FROM {fixture.Table} WHERE \"Id\"=1"));
        var literals = await provider.ExplainQueryPlanAsync(fixture.Connection,
            "SELECT (ARRAY[']'])[1], $$@unused; DELETE FROM t$$, E'quote\\\';@unused', 1 /* outer /* inner */ ; end */");
        Assert.Equal(DbaQueryPlanParameterMode.None, literals.Provenance.ParameterMode);
        Assert.All(provider.Connections, connection => Assert.Equal(ConnectionState.Closed, connection.State));
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task Capture_ReadOnlyProtectsPlanningFunctionsAndOffStringModeIsExplicit()
    {
        await using var fixture = await PostgreSqlPlanTestScope.CreateAsync();
        await fixture.ExecuteAsync($"CREATE SEQUENCE {fixture.Sequence}");
        await fixture.ExecuteAsync($"CREATE FUNCTION \"{fixture.Schema}\".advance() RETURNS bigint LANGUAGE sql IMMUTABLE AS $$ SELECT nextval('{fixture.Sequence}') $$");
        using var provider = new TrackingProvider();
        var failure = await Assert.ThrowsAsync<DbaQueryExecutionException>(() => provider.ExplainQueryPlanAsync(fixture.Connection,
            $"SELECT \"{fixture.Schema}\".advance()"));
        Assert.Equal("25006", failure.ProviderSqlState);
        Assert.Equal(false, await fixture.ScalarAsync($"SELECT is_called FROM {fixture.Sequence}"));
        var off = new NpgsqlConnectionStringBuilder(fixture.Connection) { Options = "-c standard_conforming_strings=off" };
        await Assert.ThrowsAsync<NotSupportedException>(() => provider.ExplainQueryPlanAsync(off.ConnectionString, "SELECT 1"));
        Assert.NotEmpty((await provider.ExplainQueryPlanAsync(fixture.Connection, "SELECT 1")).Steps);
        Assert.All(provider.Connections, connection => Assert.Equal(ConnectionState.Closed, connection.State));
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task Capture_ActiveAndAmbientTransactionsRemainIsolated()
    {
        await using var fixture = await PostgreSqlPlanTestScope.CreateAsync();
        using var provider = new TrackingProvider();
        await provider.BeginTransactionAsync(fixture.Connection);
        try
        {
            provider.ExecuteNonQuery(fixture.Connection, $"INSERT INTO {fixture.Table} VALUES (4001,1)", useTransaction: true);
            using var ambient = new System.Transactions.TransactionScope(System.Transactions.TransactionScopeAsyncFlowOption.Enabled);
            Assert.NotEmpty((await provider.ExplainQueryPlanAsync(fixture.Connection, $"SELECT * FROM {fixture.Table}")).Steps);
            Assert.True(provider.IsInTransaction);
            Assert.Equal(3001L, provider.ExecuteScalar(fixture.Connection, $"SELECT count(*) FROM {fixture.Table}", useTransaction: true));
            Assert.Equal(3000L, await fixture.ScalarAsync($"SELECT count(*) FROM {fixture.Table}"));
            ambient.Complete();
        }
        finally { provider.Rollback(); }
        Assert.All(provider.Connections, connection => Assert.Equal(ConnectionState.Closed, connection.State));
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task Capture_PermissionFailureClosesTheOwnedConnection()
    {
        await using var fixture = await PostgreSqlPlanTestScope.CreateAsync();
        var deniedConnection = await fixture.CreateDeniedRoleAsync();
        using var provider = new TrackingProvider();
        var failure = await Assert.ThrowsAsync<DbaQueryExecutionException>(() => provider.ExplainQueryPlanAsync(deniedConnection, $"SELECT * FROM {fixture.Table}"));
        Assert.Equal("42501", failure.ProviderSqlState);
        Assert.NotEmpty((await provider.ExplainQueryPlanAsync(fixture.Connection, $"SELECT * FROM {fixture.Table}")).Steps);
        Assert.All(provider.Connections, connection => Assert.Equal(ConnectionState.Closed, connection.State));
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task Capture_RunningCancellationAndTimeoutAreBoundedAndDoNotReplay()
    {
        await using var fixture = await PostgreSqlPlanTestScope.CreateAsync();
        await using var blocker = new NpgsqlConnection(fixture.Connection);
        await blocker.OpenAsync();
        await using var transaction = await blocker.BeginTransactionAsync();
        await using (var command = new NpgsqlCommand($"LOCK TABLE {fixture.Table} IN ACCESS EXCLUSIVE MODE", blocker, transaction))
            await command.ExecuteNonQueryAsync();
        try
        {
            using var provider = new TrackingProvider { CommandTimeout = 30, MaxRetryAttempts = 5, CommandRetryMode = CommandRetryMode.ReplaySafe };
            using var cancellation = new CancellationTokenSource();
            var capture = provider.ExplainQueryPlanAsync(fixture.Connection, $"SELECT * FROM {fixture.Table}", cancellationToken: cancellation.Token);
            await fixture.WaitForPlanLockAsync(await provider.Opened.Task.WaitAsync(TimeSpan.FromSeconds(10)));
            var elapsed = Stopwatch.StartNew();
            cancellation.Cancel();
            var failure = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => capture.WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.Equal(cancellation.Token, failure.CancellationToken);
            Assert.True(elapsed.Elapsed < TimeSpan.FromSeconds(10));
            Assert.Equal(ConnectionState.Closed, Assert.Single(provider.Connections).State);
            using var timeout = new TrackingProvider { CommandTimeout = 1, MaxRetryAttempts = 5, CommandRetryMode = CommandRetryMode.ReplaySafe };
            using var operation = DbaClientXDiagnostics.StartOperation("plan-timeout", null);
            var deadline = await Assert.ThrowsAsync<DbaQueryExecutionException>(() => timeout.ExplainQueryPlanAsync(fixture.Connection,
                $"SELECT * FROM {fixture.Table}").WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.NotNull(deadline.QueryFingerprint);
            Assert.Contains("Npgsql", deadline.ProviderExceptionType!);
            Assert.Equal(0, operation.Telemetry.RetryCount);
            Assert.Equal(ConnectionState.Closed, Assert.Single(timeout.Connections).State);
        }
        finally { await transaction.RollbackAsync(); }
        Assert.Equal(3000L, await fixture.ScalarAsync($"SELECT count(*) FROM {fixture.Table}"));
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task Capture_TransactionAndConnectionCleanupFailuresAreRetainedAndRedacted()
    {
        await using var fixture = await PostgreSqlPlanTestScope.CreateAsync();
        using var provider = new CleanupFailureProvider();
        var failure = await Assert.ThrowsAsync<AggregateException>(() => provider.ExplainQueryPlanAsync(fixture.Connection, "SELECT 1"));
        Assert.Equal(2, failure.InnerExceptions.Count);
        Assert.All(failure.InnerExceptions, error => Assert.IsType<DbaQueryExecutionException>(error));
        Assert.DoesNotContain("private-sentinel", failure.ToString());
        Assert.True(provider.ConnectionClosed);
    }

    private sealed class CleanupFailureProvider : PostgreSql
    {
        internal bool ConnectionClosed { get; private set; }
        protected override async ValueTask DisposeDbTransactionAsync(NpgsqlTransaction transaction)
        {
            await base.DisposeDbTransactionAsync(transaction);
            throw new InvalidOperationException("private-sentinel transaction");
        }
        protected override async ValueTask DisposeConnectionAsync(NpgsqlConnection connection)
        {
            await base.DisposeConnectionAsync(connection);
            ConnectionClosed = connection.State == ConnectionState.Closed;
            throw new InvalidOperationException("private-sentinel connection");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task Capture_CancellationDuringCleanupPreventsResultDelivery()
    {
        await using var fixture = await PostgreSqlPlanTestScope.CreateAsync();
        using var cancellation = new CancellationTokenSource();
        using var provider = new CleanupCancellationProvider(cancellation);
        var failure = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => provider.ExplainQueryPlanAsync(
            fixture.Connection, "SELECT 1", cancellationToken: cancellation.Token));
        Assert.Equal(cancellation.Token, failure.CancellationToken);
    }

    private sealed class CleanupCancellationProvider(CancellationTokenSource cancellation) : PostgreSql
    {
        protected override async ValueTask DisposeConnectionAsync(NpgsqlConnection connection)
        {
            await base.DisposeConnectionAsync(connection);
            cancellation.Cancel();
        }
    }

    private sealed class TrackingProvider : PostgreSql
    {
        internal List<NpgsqlConnection> Connections { get; } = new();
        internal TaskCompletionSource<int> Opened { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        protected override bool IsTransient(Exception exception) => true; // Capture must ignore replay settings even for a classified transient error.
        protected override NpgsqlConnection CreateConnection(string connectionString)
        {
            var connection = base.CreateConnection(connectionString);
            Connections.Add(connection);
            return connection;
        }
        protected override async Task OpenConnectionAsync(NpgsqlConnection connection, CancellationToken token)
        {
            await base.OpenConnectionAsync(connection, token);
            Opened.TrySetResult(connection.ProcessID);
        }
    }
}
