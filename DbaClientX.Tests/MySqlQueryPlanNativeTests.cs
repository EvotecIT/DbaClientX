using System.Data;
using DBAClientX;
using DBAClientX.QueryPlans;
using MySqlConnector;

namespace DbaClientX.Tests;

public sealed class MySqlQueryPlanNativeTests
{
    [Theory]
    [InlineData("DBACLIENTX_MYSQL_PLAN_TEST_CONNECTION", MySqlQueryPlanFormat.MySqlJsonV1)]
    [InlineData("DBACLIENTX_MARIADB_PLAN_TEST_CONNECTION", MySqlQueryPlanFormat.MariaDbJson)]
    public async Task Native_BoundPlansPreserveNativeIdentityAndNeverExecuteDml(string setting, MySqlQueryPlanFormat format)
    {
        await using var fixture = await Fixture.CreateAsync(setting);
        var client = new MySql();
        var values = new Dictionary<string, object?> { ["id"] = 1 };
        var types = new Dictionary<string, MySqlDbType> { ["id"] = MySqlDbType.Int32 };
        var plan = await client.ExplainQueryPlanAsync(fixture.ConnectionString,
            "SELECT NativeAlias.id FROM Events AS NativeAlias WHERE NativeAlias.id = @id", values, types);
        Assert.Equal(format == MySqlQueryPlanFormat.MySqlJsonV1 ? "MySQL EXPLAIN JSON v1" : "MariaDB EXPLAIN JSON", plan.Provenance.Format);
        Assert.Equal(DbaQueryPlanParameterMode.BoundValues, plan.Provenance.ParameterMode);
        Assert.Contains(plan.Steps, step => step.Table == "NativeAlias" && step.Operation == DbaQueryPlanOperation.Search);
        Assert.All(plan.Steps, step => { Assert.Null(step.Alias); Assert.Null(step.Schema); Assert.Null(step.Estimates!.TableRows); Assert.Null(step.Estimates.OutputRows); });
        var nullPlan = client.ExplainQueryPlan(fixture.ConnectionString, "SELECT * FROM Events WHERE id=@id",
            new Dictionary<string, object?> { ["id"] = null }, types);
        Assert.NotEmpty(nullPlan.Steps);
        foreach (var sql in new[] { "UPDATE Events SET payload=99 WHERE id=@id", "DELETE FROM Events WHERE id=@id",
            "INSERT INTO Events(id,payload) VALUES(99,@id)", "REPLACE INTO Events(id,payload) VALUES(1,@id)" })
            await Assert.ThrowsAsync<ArgumentException>(() => client.ExplainQueryPlanAsync(fixture.ConnectionString, sql, values, types));
        Assert.Equal(30L, Convert.ToInt64(await fixture.ScalarAsync("SELECT COUNT(*) FROM Events")));
        Assert.Equal(7L, Convert.ToInt64(await fixture.ScalarAsync("SELECT payload FROM Events WHERE id=1")));
        var composed = await client.ExplainQueryPlanAsync(fixture.ConnectionString,
            "WITH data AS (SELECT id FROM Events WHERE id<20) SELECT a.id, COUNT(b.id) FROM data a JOIN Events b ON a.id=b.id GROUP BY a.id ORDER BY a.id DESC");
        Assert.True(composed.Steps.Count > 2);
        Assert.Equal(DbaQueryPlanParameterMode.None, composed.Provenance.ParameterMode);
        var union = await client.ExplainQueryPlanAsync(fixture.ConnectionString,
            "SELECT id FROM Events WHERE id<3 UNION SELECT id FROM Events WHERE id>28");
        Assert.Contains(union.Steps, step => step.Detail.StartsWith("union_result", StringComparison.Ordinal));
        Assert.True(union.Steps.Count(step => step.Table == "Events") >= 2);
        Assert.NotEmpty((await client.ExplainQueryPlanAsync(fixture.ConnectionString,
            "SELECT SUM(payload) OVER(ORDER BY id) FROM Events")).Steps);
        using var tls = new MySqlConnection(fixture.ConnectionString);
        await tls.OpenAsync();
        using var command = new MySqlCommand("SHOW SESSION STATUS LIKE 'Ssl_cipher'", tls);
        using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.NotEmpty(reader.GetString(1));
    }

    [Theory]
    [InlineData("DBACLIENTX_MYSQL_PLAN_TEST_CONNECTION")]
    [InlineData("DBACLIENTX_MARIADB_PLAN_TEST_CONNECTION")]
    public async Task Native_CancellationAfterObservedLockReleasesOwnedSession(string setting)
    {
        await using var fixture = await Fixture.CreateAsync(setting);
        using var blocker = new MySqlConnection(fixture.ConnectionString);
        await blocker.OpenAsync();
        using (var command = new MySqlCommand("LOCK TABLES Events WRITE", blocker)) await command.ExecuteNonQueryAsync();
        using var cancellation = new CancellationTokenSource();
        var client = new ObservedClient();
        var capture = client.ExplainQueryPlanAsync(fixture.ConnectionString, "SELECT * FROM Events", cancellationToken: cancellation.Token);
        try
        {
            var observed = false;
            var deadline = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < deadline && !capture.IsCompleted)
            {
                var count = Convert.ToInt64(await fixture.ScalarAsync("SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE ID="
                    + client.ThreadId + " AND INFO LIKE 'EXPLAIN FORMAT=JSON%' AND (STATE LIKE '%lock%' OR STATE='Locked')"));
                if (count == 1) { observed = true; break; }
                await Task.Delay(50);
            }
            Assert.True(observed, "The unique owned EXPLAIN must be observed waiting before cancellation.");
            cancellation.Cancel();
            var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => capture);
            Assert.Equal(cancellation.Token, error.CancellationToken);
            Assert.Equal(1, client.Disposed);
            Assert.Equal(0L, Convert.ToInt64(await fixture.ScalarAsync("SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE ID=" + client.ThreadId)));
        }
        finally
        {
            cancellation.Cancel();
            using var unlock = new MySqlCommand("UNLOCK TABLES", blocker);
            await unlock.ExecuteNonQueryAsync();
            try { await capture; } catch (Exception) { }
        }
    }

    [Theory]
    [InlineData("DBACLIENTX_MYSQL_PLAN_TEST_CONNECTION", "ANSI_QUOTES")]
    [InlineData("DBACLIENTX_MYSQL_PLAN_TEST_CONNECTION", "NO_BACKSLASH_ESCAPES")]
    [InlineData("DBACLIENTX_MARIADB_PLAN_TEST_CONNECTION", "ANSI_QUOTES")]
    [InlineData("DBACLIENTX_MARIADB_PLAN_TEST_CONNECTION", "NO_BACKSLASH_ESCAPES")]
    public async Task Native_UnsafeSessionModeFailsBeforeExplainingAndCloses(string setting, string mode)
    {
        await using var fixture = await Fixture.CreateAsync(setting);
        var client = new ObservedClient(mode);
        await Assert.ThrowsAsync<NotSupportedException>(() => client.ExplainQueryPlanAsync(fixture.ConnectionString, "SELECT 1"));
        Assert.Equal(1, client.Disposed);
    }

    [Theory]
    [InlineData("DBACLIENTX_MYSQL_PLAN_TEST_CONNECTION")]
    [InlineData("DBACLIENTX_MARIADB_PLAN_TEST_CONNECTION")]
    public async Task Native_LateCancellationBlocksDeliveryAndClientTransactionStaysOwned(string setting)
    {
        await using var fixture = await Fixture.CreateAsync(setting);
        var client = new MySql();
        await client.BeginTransactionAsync(fixture.ConnectionString);
        try
        {
            Assert.NotEmpty((await client.ExplainQueryPlanAsync(fixture.ConnectionString, "SELECT 1")).Steps);
            Assert.True(client.IsInTransaction);
            await client.RollbackAsync();
        }
        finally { if (client.IsInTransaction) await client.RollbackAsync(); }
        using var cancellation = new CancellationTokenSource();
        var late = new ObservedClient(cancelOnClose: cancellation);
        var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => late.ExplainQueryPlanAsync(
            fixture.ConnectionString, "SELECT 1", cancellationToken: cancellation.Token));
        Assert.Equal(cancellation.Token, error.CancellationToken);
        Assert.Equal(1, late.Disposed);
    }

    private sealed class ObservedClient(string? mode = null, CancellationTokenSource? cancelOnClose = null) : MySql
    {
        public int ThreadId, Disposed;
        protected override async Task OpenConnectionAsync(MySqlConnection connection, CancellationToken token)
        {
            await base.OpenConnectionAsync(connection, token).ConfigureAwait(false);
            ThreadId = connection.ServerThread;
            if (mode != null)
            {
                using var command = new MySqlCommand("SET SESSION sql_mode='" + mode + "'", connection);
                await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
            }
        }
        protected override async ValueTask DisposeConnectionAsync(MySqlConnection connection)
        {
            await base.DisposeConnectionAsync(connection).ConfigureAwait(false);
            Disposed++;
            cancelOnClose?.Cancel();
        }
    }

    private sealed class Fixture(string admin, string database) : IAsyncDisposable
    {
        public string ConnectionString { get; } = new MySqlConnectionStringBuilder(admin) { Database = database, Pooling = false, AutoEnlist = false }.ConnectionString;
        public static async Task<Fixture> CreateAsync(string setting)
        {
            var connection = Environment.GetEnvironmentVariable(setting);
            Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), setting + " is required for native estimated-plan qualification.");
            var fixture = new Fixture(connection!, "dbx_plan_" + Guid.NewGuid().ToString("N"));
            try
            {
                await fixture.AdminAsync("CREATE DATABASE `" + databaseName(fixture) + "`");
                await fixture.ScalarAsync("CREATE TABLE Events(id INT PRIMARY KEY,payload INT NOT NULL)");
                await fixture.ScalarAsync("INSERT INTO Events VALUES " + string.Join(",", Enumerable.Range(1, 30).Select(id => "(" + id + ",7)")));
                return fixture;
            }
            catch { await fixture.DisposeAsync(); throw; }
        }
        private static string databaseName(Fixture fixture) => new MySqlConnectionStringBuilder(fixture.ConnectionString).Database;
        public async Task<object?> ScalarAsync(string sql)
        {
            using var connection = new MySqlConnection(ConnectionString);
            await connection.OpenAsync();
            using var command = new MySqlCommand(sql, connection) { CommandTimeout = 20 };
            return await command.ExecuteScalarAsync();
        }
        private async Task AdminAsync(string sql)
        {
            using var connection = new MySqlConnection(new MySqlConnectionStringBuilder(admin) { Pooling = false, AutoEnlist = false }.ConnectionString);
            await connection.OpenAsync();
            using var command = new MySqlCommand(sql, connection) { CommandTimeout = 20 };
            await command.ExecuteNonQueryAsync();
        }
        public async ValueTask DisposeAsync() => await AdminAsync("DROP DATABASE IF EXISTS `" + database + "`");
    }
}
