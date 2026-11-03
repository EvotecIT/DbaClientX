using System.Data;
using DBAClientX;
using Microsoft.Data.SqlClient;
using Xunit;

namespace DbaClientX.Tests;

public sealed class SqlServerSessionTransactionTests
{
    [Fact]
    public async Task ExecuteInTransactionAsync_ErrorAfterAdditionalResult_RollsBackEarlierWrite()
    {
        await using Fixture fixture = await Fixture.CreateAsync();
        using SqlServer client = new();
        await using SqlServerSessionLock held = await client.AcquireSessionLockAsync(fixture.ConnectionString, fixture.Resource);

        await Assert.ThrowsAsync<InvalidOperationException>(() => held.ExecuteInTransactionAsync(async (scope, ct) =>
        {
            await scope.ExecuteNonQueryAsync($"INSERT INTO {fixture.Table}(Value) VALUES(7);", cancellationToken: ct);
            await Assert.ThrowsAsync<SqlException>(() => scope.QueryAsync(
                "SELECT 1 AS FirstResult; SELECT 2 AS SecondResult; RAISERROR('Later operation failed', 16, 1);",
                cancellationToken: ct));
        }));

        Assert.Equal(0, await fixture.CountAsync());
        await Assert.ThrowsAsync<InvalidOperationException>(() => held.DemandHeldAsync());
    }

    [Fact]
    public async Task ExecuteInTransactionAsync_Success_CommitsOnLockedSessionAndClosesScope()
    {
        await using Fixture fixture = await Fixture.CreateAsync();
        using SqlServer client = new();
        using SqlServer competitor = new();
        await using SqlServerSessionLock held = await client.AcquireSessionLockAsync(fixture.ConnectionString, fixture.Resource);
        SqlServerSessionTransaction? escaped = null;
        int firstSession = 0;
        await held.ExecuteInTransactionAsync(async (scope, ct) =>
        {
            escaped = scope;
            DataTable identity = await scope.QueryAsync("SELECT @@SPID AS SessionId, @@TRANCOUNT AS Transactions;", cancellationToken: ct);
            firstSession = Convert.ToInt32(identity.Rows[0]["SessionId"]);
            Assert.Equal(1, (int)identity.Rows[0]["Transactions"]);
            await Assert.ThrowsAsync<InvalidOperationException>(() => competitor.AcquireSessionLockAsync(fixture.ConnectionString, fixture.Resource));
            Assert.Equal(1, await scope.ExecuteNonQueryAsync($"INSERT INTO {fixture.Table}(Value) VALUES(@value);",
                new Dictionary<string, object?> { ["@value"] = 7 }, ct));
        });
        Assert.Equal(1, await fixture.CountAsync());
        Assert.NotNull(escaped);
        await Assert.ThrowsAsync<InvalidOperationException>(() => escaped!.ExecuteNonQueryAsync($"DELETE FROM {fixture.Table};"));
        await held.ExecuteInTransactionAsync(async (scope, ct) =>
        {
            DataTable result = await scope.QueryAsync("SELECT @@SPID AS SessionId;", cancellationToken: ct);
            Assert.Equal(firstSession, Convert.ToInt32(result.Rows[0]["SessionId"]));
        });
        await held.DemandHeldAsync();
    }

    [Fact]
    public async Task ExecuteInTransactionAsync_SwallowedCommandFailure_RollsBackAndInvalidatesSession()
    {
        await using Fixture fixture = await Fixture.CreateAsync();
        using SqlServer client = new();
        await using SqlServerSessionLock held = await client.AcquireSessionLockAsync(fixture.ConnectionString, fixture.Resource);
        await Assert.ThrowsAsync<InvalidOperationException>(() => held.ExecuteInTransactionAsync(async (scope, ct) =>
        {
            await scope.ExecuteNonQueryAsync($"INSERT INTO {fixture.Table}(Value) VALUES(7);", cancellationToken: ct);
            await Assert.ThrowsAnyAsync<Exception>(() => scope.ExecuteNonQueryAsync(
                "THROW 51000, 'Disposable transaction failure', 1;", cancellationToken: ct));
        }));
        Assert.Equal(0, await fixture.CountAsync());
        await Assert.ThrowsAsync<InvalidOperationException>(() => held.DemandHeldAsync());
        await using SqlServerSessionLock successor = await client.AcquireSessionLockAsync(fixture.ConnectionString, fixture.Resource);
        await successor.DemandHeldAsync();
    }

    [Fact]
    public async Task ExecuteInTransactionAsync_OriginalSessionClosed_RollsBackAndCannotWriteAfterSuccessor()
    {
        await using Fixture fixture = await Fixture.CreateAsync();
        SqlConnection? connection = null;
        using SqlServer client = new()
        {
            ConnectionOptions = new SqlServerConnectionOptions { ConnectionFactory = value => connection = new SqlConnection(value) }
        };
        using SqlServer competitor = new();
        await using SqlServerSessionLock held = await client.AcquireSessionLockAsync(fixture.ConnectionString, fixture.Resource);
        await Assert.ThrowsAnyAsync<Exception>(() => held.ExecuteInTransactionAsync(async (scope, ct) =>
        {
            await scope.ExecuteNonQueryAsync($"INSERT INTO {fixture.Table}(Value) VALUES(7);", cancellationToken: ct);
            Assert.NotNull(connection);
            await connection!.CloseAsync();
            await using SqlServerSessionLock successor = await competitor.AcquireSessionLockAsync(fixture.ConnectionString, fixture.Resource);
            await successor.DemandHeldAsync();
            await scope.ExecuteNonQueryAsync($"INSERT INTO {fixture.Table}(Value) VALUES(8);", cancellationToken: ct);
        }));
        Assert.Equal(0, await fixture.CountAsync());
        await Assert.ThrowsAsync<InvalidOperationException>(() => held.DemandHeldAsync());
    }

    [Fact]
    public async Task ExecuteInTransactionAsync_CancelledCommand_RollsBackWithoutReplaying()
    {
        await using Fixture fixture = await Fixture.CreateAsync();
        using SqlServer client = new();
        await using SqlServerSessionLock held = await client.AcquireSessionLockAsync(fixture.ConnectionString, fixture.Resource);
        using CancellationTokenSource cancelled = new();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => held.ExecuteInTransactionAsync(async (scope, ct) =>
        {
            await scope.ExecuteNonQueryAsync($"INSERT INTO {fixture.Table}(Value) VALUES(7);", cancellationToken: ct);
            cancelled.CancelAfter(TimeSpan.FromMilliseconds(200));
            await scope.QueryAsync("WAITFOR DELAY '00:00:20'; SELECT 1;", cancellationToken: ct);
        }, cancellationToken: cancelled.Token));
        Assert.Equal(0, await fixture.CountAsync());
        await Assert.ThrowsAsync<InvalidOperationException>(() => held.DemandHeldAsync());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ExecuteInTransactionAsync_SwallowedPreCancelledCommand_RollsBack(bool query)
    {
        await using Fixture fixture = await Fixture.CreateAsync();
        using SqlServer client = new();
        await using SqlServerSessionLock held = await client.AcquireSessionLockAsync(fixture.ConnectionString, fixture.Resource);
        using CancellationTokenSource cancelled = new();
        cancelled.Cancel();
        await Assert.ThrowsAsync<InvalidOperationException>(() => held.ExecuteInTransactionAsync(async (scope, ct) =>
        {
            await scope.ExecuteNonQueryAsync($"INSERT INTO {fixture.Table}(Value) VALUES(7);", cancellationToken: ct);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            {
                if (query) await scope.QueryAsync("SELECT 1;", cancellationToken: cancelled.Token);
                else await scope.ExecuteNonQueryAsync($"INSERT INTO {fixture.Table}(Value) VALUES(8);", cancellationToken: cancelled.Token);
            });
        }));
        Assert.Equal(0, await fixture.CountAsync());
        await Assert.ThrowsAsync<InvalidOperationException>(() => held.DemandHeldAsync());
    }

    [Fact]
    public async Task ExecuteInTransactionAsync_SwallowedQueuedCancellation_RollsBack()
    {
        await using Fixture fixture = await Fixture.CreateAsync();
        using SqlServer client = new();
        await using SqlServerSessionLock held = await client.AcquireSessionLockAsync(fixture.ConnectionString, fixture.Resource);
        using CancellationTokenSource cancelled = new();
        await Assert.ThrowsAsync<InvalidOperationException>(() => held.ExecuteInTransactionAsync(async (scope, ct) =>
        {
            await scope.ExecuteNonQueryAsync($"INSERT INTO {fixture.Table}(Value) VALUES(7);", cancellationToken: ct);
            Task<DataTable> executing = scope.QueryAsync("WAITFOR DELAY '00:00:02'; SELECT 1;", cancellationToken: ct);
            Assert.False(executing.IsCompleted);
            Task<int> queued = scope.ExecuteNonQueryAsync($"INSERT INTO {fixture.Table}(Value) VALUES(8);", cancellationToken: cancelled.Token);
            Assert.False(queued.IsCompleted);
            cancelled.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => queued);
            await executing;
        }));
        Assert.Equal(0, await fixture.CountAsync());
        await Assert.ThrowsAsync<InvalidOperationException>(() => held.DemandHeldAsync());
    }

    private sealed class Fixture : IAsyncDisposable
    {
        private readonly SqlConnection _anchor;
        private Fixture(string connectionString, SqlConnection anchor, string id)
        {
            ConnectionString = connectionString;
            _anchor = anchor;
            Resource = "DbaClientX-SessionTransaction-" + id;
            Table = "##DbaClientXSessionTransaction_" + id;
        }
        internal string ConnectionString { get; }
        internal string Resource { get; }
        internal string Table { get; }

        internal static async Task<Fixture> CreateAsync()
        {
            string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_SESSION_LOCK_TEST_CONNECTION");
            Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Set a disposable SQL Server session-lock test connection.");
            SqlConnection anchor = new(connectionString);
            try
            {
                await anchor.OpenAsync();
                Fixture fixture = new(connectionString!, anchor, Guid.NewGuid().ToString("N"));
                using SqlCommand create = anchor.CreateCommand();
                create.CommandText = $"CREATE TABLE {fixture.Table}(Value int NOT NULL);";
                await create.ExecuteNonQueryAsync();
                return fixture;
            }
            catch { await anchor.DisposeAsync(); throw; }
        }

        internal async Task<int> CountAsync()
        {
            using SqlCommand query = _anchor.CreateCommand();
            query.CommandText = $"SELECT COUNT(*) FROM {Table};";
            return (int)(await query.ExecuteScalarAsync())!;
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                using SqlCommand cleanup = _anchor.CreateCommand();
                cleanup.CommandText = $"DROP TABLE {Table}; SELECT OBJECT_ID('tempdb..{Table}');";
                Assert.Equal(DBNull.Value, await cleanup.ExecuteScalarAsync());
            }
            finally { await _anchor.DisposeAsync(); }
        }
    }
}
