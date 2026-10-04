using System.Data;
using DBAClientX;
using DBAClientX.QueryPlans;
using Oracle.ManagedDataAccess.Client;

namespace DbaClientX.Tests;

public sealed class OracleQueryPlanNativeTests
{
    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task Native_BindsWithOnlySessionPrivilegeAndRollsBackOwnedRows()
    {
        await using var fixture = await Scope.CreateAsync();
        using var provider = new TrackingProvider();
        var plan = await provider.ExplainQueryPlanAsync(fixture.Connection,
            "WITH x AS (SELECT :tenant$Id# AS value, nq'[; :ignored]' AS label FROM dual) SELECT value FROM x",
            new Dictionary<string, object?> { [":tenant$Id#"] = 42 }, new Dictionary<string, OracleDbType> { ["tenant$Id#"] = OracleDbType.Int32 });
        Assert.NotEmpty(plan.Steps); Assert.Equal("PLAN_TABLE", plan.Provenance.Format);
        Assert.Equal(DbaQueryPlanParameterMode.BoundValues, plan.Provenance.ParameterMode);
        Assert.Equal(0, provider.RemainingRows); Assert.Equal(ConnectionState.Closed, provider.Connection!.State);
        Assert.All(plan.Steps, step => { Assert.Null(step.Database); Assert.Null(step.Estimates!.RowsRead); Assert.Null(step.Estimates.TableRows); });
        Assert.NotEmpty(provider.ExplainQueryPlan(fixture.Connection, "SELECT :id FROM dual",
            new Dictionary<string, object?> { ["id"] = null }, new Dictionary<string, OracleDbType> { ["id"] = OracleDbType.NVarchar2 }).Steps);
        Assert.Equal(0L, Convert.ToInt64(await fixture.ScalarAsync("SELECT COUNT(*) FROM USER_TABLES")));
        Assert.Equal(1L, Convert.ToInt64(await fixture.ScalarAsync("SELECT COUNT(*) FROM USER_SYS_PRIVS")));
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task Native_PreservesPhysicalTableIndexAndLeavesApplicationTransactionUnchanged()
    {
        await using var fixture = await Scope.CreateAsync();
        await fixture.CreateTableAsync();
        using var provider = new TrackingProvider();
        await provider.BeginTransactionAsync(fixture.Connection);
        await provider.ExecuteNonQueryAsync(fixture.Connection, "UPDATE \"Events.Case\" SET \"Payload\"=99 WHERE \"Id\"=1", useTransaction: true);
        var plan = await provider.ExplainQueryPlanAsync(fixture.Connection,
            "SELECT /*+ INDEX(e \"Events.Key\") */ e.\"Payload\" FROM \"Events.Case\" e WHERE e.\"Id\"=:id",
            new Dictionary<string, object?> { ["id"] = 1 }, new Dictionary<string, OracleDbType> { ["id"] = OracleDbType.Int32 });
        Assert.Contains(plan.Steps, step => step.Index == "Events.Key" && step.Table == null);
        Assert.Contains(plan.Steps, step => step.Table == "Events.Case" && step.Schema == fixture.User);
        Assert.Equal(99m, await provider.ExecuteScalarAsync(fixture.Connection, "SELECT \"Payload\" FROM \"Events.Case\" WHERE \"Id\"=1", useTransaction: true));
        Assert.Equal(7m, await fixture.ScalarAsync("SELECT \"Payload\" FROM \"Events.Case\" WHERE \"Id\"=1"));
        await provider.RollbackAsync();
        await Assert.ThrowsAsync<ArgumentException>(() => provider.ExplainQueryPlanAsync(fixture.Connection, "WITH x AS (SELECT 1 FROM dual) DELETE FROM \"Events.Case\""));
        Assert.Equal(7m, await fixture.ScalarAsync("SELECT \"Payload\" FROM \"Events.Case\" WHERE \"Id\"=1"));
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task Native_SnapshotsBindingsDuringOpenAndNormalizesCallerCancellation()
    {
        await using var fixture = await Scope.CreateAsync();
        using var provider = new PausedOpenProvider();
        var values = new Dictionary<string, object?> { ["id"] = 42 };
        var types = new Dictionary<string, OracleDbType> { ["id"] = OracleDbType.Int32 };
        var operation = provider.ExplainQueryPlanAsync(fixture.Connection, "SELECT :id FROM dual", values, types);
        await provider.Opened.Task.WaitAsync(TimeSpan.FromSeconds(10));
        values.Clear(); types.Clear(); provider.Continue.TrySetResult();
        Assert.NotEmpty((await operation).Steps);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => provider.ExplainQueryPlanAsync(fixture.Connection, "SELECT 1 FROM dual", cancellationToken: cancellation.Token));
        Assert.Equal(cancellation.Token, error.CancellationToken);
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task Native_LateCancellationAndCleanupFailuresPreventDeliveryWithoutLeakingRowsOrProviderText()
    {
        await using var fixture = await Scope.CreateAsync();
        using var cancellation = new CancellationTokenSource();
        using var canceled = new CleanupProvider(cancellation, false);
        var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => canceled.ExplainQueryPlanAsync(fixture.Connection, "SELECT 1 FROM dual", cancellationToken: cancellation.Token));
        Assert.Equal(cancellation.Token, error.CancellationToken); Assert.Equal(0, canceled.RemainingRows);
        using var failure = new CleanupProvider(null, true);
        var aggregate = await Assert.ThrowsAsync<AggregateException>(() => failure.ExplainQueryPlanAsync(fixture.Connection, "SELECT 1 FROM dual"));
        Assert.Equal(2, aggregate.InnerExceptions.Count); Assert.DoesNotContain("private-sentinel", aggregate.ToString());
        Assert.Equal(ConnectionState.Closed, failure.Connection!.State);
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task Native_CancelsAnObservedExplainWithoutReplayingAndReleasesTheSession()
    {
        await using var fixture = await Scope.CreateAsync();
        await fixture.CreateTableAsync();
        using var provider = new TrackingProvider { CommandRetryMode = CommandRetryMode.ReplaySafe, MaxRetryAttempts = 5, RetryNonQueryOperations = true };
        using var cancellation = new CancellationTokenSource();
        string sql = string.Join(" UNION ALL ", Enumerable.Range(0, 1200).Select(value =>
            "SELECT \"Id\" FROM \"Events.Case\" WHERE \"Id\"=" + value));
        var operation = provider.ExplainQueryPlanAsync(fixture.Connection, sql, cancellationToken: cancellation.Token);
        bool observed = false;
        try
        {
            var deadline = DateTime.UtcNow.AddSeconds(15);
            while (!operation.IsCompleted && DateTime.UtcNow < deadline)
            {
                if (await fixture.ExplainActiveAsync()) { observed = true; break; }
                await Task.Delay(10);
            }
            Assert.True(observed, "A unique native EXPLAIN must be active before caller cancellation.");
            cancellation.Cancel();
            var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => operation.WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.Equal(cancellation.Token, error.CancellationToken);
            Assert.Equal(1, provider.Created);
            Assert.Equal(0, provider.TransientChecks);
            Assert.Equal(ConnectionState.Closed, provider.Connection!.State);
            Assert.Equal(0, provider.RemainingRows);
            Assert.Equal(0, await fixture.SessionCountAsync());
            Assert.Equal(7m, await fixture.ScalarAsync("SELECT \"Payload\" FROM \"Events.Case\" WHERE \"Id\"=1"));
        }
        finally
        {
            cancellation.Cancel();
            try { await operation; } catch (Exception) { }
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task Native_TypedBindConversionFailuresKeepSensitiveValuesOutOfPublicErrors()
    {
        await using var fixture = await Scope.CreateAsync();
        using var provider = new TrackingProvider();
        var error = await Assert.ThrowsAsync<DbaQueryExecutionException>(() => provider.ExplainQueryPlanAsync(fixture.Connection, "SELECT :id FROM dual",
            new Dictionary<string, object?> { ["id"] = "private-sentinel-bind-value" }, new Dictionary<string, OracleDbType> { ["id"] = OracleDbType.Int32 }));
        Assert.DoesNotContain("private-sentinel", error.ToString());
        Assert.Equal(0, provider.RemainingRows); Assert.Equal(ConnectionState.Closed, provider.Connection!.State);
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task Native_PreservesHashIdentifiersDatabaseLinksAndIndexScanAssessment()
    {
        await using var fixture = await Scope.CreateAsync();
        await fixture.CreateTableAsync();
        await fixture.CreateDatabaseLinkAsync();
        using var provider = new TrackingProvider();
        Assert.NotEmpty((await provider.ExplainQueryPlanAsync(fixture.Connection, "SELECT 1 AS merge# FROM dual")).Steps);
        var remote = await provider.ExplainQueryPlanAsync(fixture.Connection, "SELECT \"Id\" FROM \"Events.Case\"@DBAX_LOOP");
        Assert.Contains(remote.Steps, step => step.Detail.EndsWith("REMOTE", StringComparison.Ordinal));
        var plan = await provider.ExplainQueryPlanAsync(fixture.Connection, "SELECT /*+ INDEX_FFS(e \"Events.Key\") */ e.\"Id\" FROM \"Events.Case\" e");
        Assert.Contains(plan.ScanOperations, step => step.Index == "Events.Key" && step.Table == null);
        Assert.Equal(0, provider.RemainingRows);
    }

    private class TrackingProvider : DBAClientX.Oracle
    {
        internal OracleConnection? Connection; internal int RemainingRows = -1; internal int Created; internal int TransientChecks;
        protected override bool IsTransient(Exception error) { TransientChecks++; return true; }
        protected override OracleConnection CreateConnection(string connectionString)
        { Created++; Connection = base.CreateConnection(connectionString); return Connection; }
        protected override async ValueTask DisposeDbTransactionAsync(OracleTransaction transaction)
        {
            if (transaction.Connection?.State == ConnectionState.Open)
            {
                using var command = new OracleCommand("SELECT COUNT(*) FROM SYS.PLAN_TABLE$", transaction.Connection) { CommandTimeout = 5 };
                RemainingRows = Convert.ToInt32(await command.ExecuteScalarAsync());
            }
            await base.DisposeDbTransactionAsync(transaction);
        }
    }

    private sealed class PausedOpenProvider : TrackingProvider
    {
        internal TaskCompletionSource Opened { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource Continue { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        protected override async Task OpenConnectionAsync(OracleConnection connection, CancellationToken token)
        { await base.OpenConnectionAsync(connection, token); Opened.TrySetResult(); await Continue.Task; }
    }

    private sealed class CleanupProvider(CancellationTokenSource? cancellation, bool fail) : TrackingProvider
    {
        protected override async ValueTask DisposeDbTransactionAsync(OracleTransaction transaction)
        { await base.DisposeDbTransactionAsync(transaction); if (fail) throw new InvalidOperationException("private-sentinel transaction"); }
        protected override async ValueTask DisposeConnectionAsync(OracleConnection connection)
        { await base.DisposeConnectionAsync(connection); cancellation?.Cancel(); if (fail) throw new InvalidOperationException("private-sentinel connection"); }
    }

    private sealed class Scope : IAsyncDisposable
    {
        private readonly OracleConnection _administrator;
        internal string User { get; } = "DBAXPLAN_" + Guid.NewGuid().ToString("N")[..12].ToUpperInvariant();
        internal string Connection { get; private set; } = "";
        private Scope(string connection) { _administrator = new OracleConnection(connection); }
        internal static async Task<Scope> CreateAsync()
        {
            string? connection = Environment.GetEnvironmentVariable("DBACLIENTX_ORACLE_PLAN_TEST_CONNECTION");
            Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set DBACLIENTX_ORACLE_PLAN_TEST_CONNECTION to an isolated TCPS Oracle administrator with create/drop user authority.");
            var scope = new Scope(connection!);
            await scope._administrator.OpenAsync();
            string password = "Dbax" + Guid.NewGuid().ToString("N");
            using var command = scope._administrator.CreateCommand();
            command.CommandText = "CREATE USER " + scope.User + " IDENTIFIED BY \"" + password + "\"";
            await command.ExecuteNonQueryAsync();
            try
            {
                command.CommandText = "GRANT CREATE SESSION TO " + scope.User; await command.ExecuteNonQueryAsync();
                var builder = new OracleConnectionStringBuilder(connection) { UserID = scope.User, Password = password, Pooling = false, Enlist = "false" };
                builder["DBA Privilege"] = ""; scope.Connection = builder.ConnectionString;
                return scope;
            }
            catch { await scope.DisposeAsync(); throw; }
        }
        internal async Task CreateTableAsync()
        {
            using var grant = _administrator.CreateCommand(); grant.CommandText = "GRANT CREATE TABLE TO " + User; await grant.ExecuteNonQueryAsync();
            grant.CommandText = "ALTER USER " + User + " QUOTA 2M ON USERS"; await grant.ExecuteNonQueryAsync();
            using var connection = new OracleConnection(Connection); await connection.OpenAsync();
            using var command = connection.CreateCommand();
            foreach (string sql in new[] { "CREATE TABLE \"Events.Case\" (\"Id\" NUMBER PRIMARY KEY USING INDEX (CREATE UNIQUE INDEX \"Events.Key\" ON \"Events.Case\"(\"Id\")), \"Payload\" NUMBER)",
                "INSERT INTO \"Events.Case\" VALUES(1,7)", "COMMIT" })
            { command.CommandText = sql; await command.ExecuteNonQueryAsync(); }
        }
        internal async Task CreateDatabaseLinkAsync()
        {
            using var grant = _administrator.CreateCommand(); grant.CommandText = "GRANT CREATE DATABASE LINK TO " + User; await grant.ExecuteNonQueryAsync();
            using var connection = new OracleConnection(Connection); await connection.OpenAsync();
            using var command = connection.CreateCommand();
            var builder = new OracleConnectionStringBuilder(Connection);
            // The fixture must supply a descriptor reachable by the Oracle server, whose trust configuration is fixture-owned.
            string? source = Environment.GetEnvironmentVariable("DBACLIENTX_ORACLE_PLAN_LOOPBACK_DATA_SOURCE");
            Assert.SkipWhen(string.IsNullOrWhiteSpace(source), "Set DBACLIENTX_ORACLE_PLAN_LOOPBACK_DATA_SOURCE for native database-link qualification.");
            command.CommandText = "CREATE DATABASE LINK DBAX_LOOP CONNECT TO " + User + " IDENTIFIED BY \"" + builder.Password
                + "\" USING '" + source!.Replace("'", "''") + "'";
            await command.ExecuteNonQueryAsync();
            command.CommandText = "SELECT COUNT(*) FROM \"Events.Case\"@DBAX_LOOP";
            Assert.Equal(1, Convert.ToInt32(await command.ExecuteScalarAsync()));
        }
        internal async Task<object?> ScalarAsync(string sql)
        {
            using var connection = new OracleConnection(Connection); await connection.OpenAsync();
            using var command = new OracleCommand(sql, connection) { CommandTimeout = 5 }; return await command.ExecuteScalarAsync();
        }
        internal async Task<bool> ExplainActiveAsync()
        {
            using var command = _administrator.CreateCommand();
            command.CommandText = "SELECT COUNT(*) FROM V$SESSION s JOIN V$SQL q ON s.SQL_ID=q.SQL_ID AND s.SQL_CHILD_NUMBER=q.CHILD_NUMBER "
                + "WHERE s.USERNAME='" + User + "' AND s.STATUS='ACTIVE' AND q.SQL_TEXT LIKE 'EXPLAIN PLAN SET STATEMENT_ID=%'";
            return Convert.ToInt32(await command.ExecuteScalarAsync()) > 0;
        }
        internal async Task<int> SessionCountAsync()
        {
            using var command = _administrator.CreateCommand(); command.CommandText = "SELECT COUNT(*) FROM V$SESSION WHERE USERNAME='" + User + "'";
            return Convert.ToInt32(await command.ExecuteScalarAsync());
        }
        public async ValueTask DisposeAsync()
        {
            try
            {
                if (_administrator.State == ConnectionState.Open)
                {
                    using var command = _administrator.CreateCommand(); command.CommandText = "DROP USER " + User + " CASCADE"; await command.ExecuteNonQueryAsync();
                    command.CommandText = "SELECT COUNT(*) FROM DBA_USERS WHERE USERNAME='" + User + "'";
                    Assert.Equal(0, Convert.ToInt32(await command.ExecuteScalarAsync()));
                }
            }
            finally { await _administrator.DisposeAsync(); }
        }
    }
}
