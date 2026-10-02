using System.Diagnostics;

namespace DbaClientX.Tests;

/// <summary>
/// A canceled token stops a statement the server is running, for the providers whose drivers cancel on the server
/// (SQL Server attention, PostgreSQL cancel request, MySQL KILL QUERY). SQLite is covered by
/// <see cref="SQLiteStatementInterruptTests"/>; Oracle has no local lane.
/// </summary>
public sealed class ProviderStatementCancellationLiveTests
{
    private static readonly TimeSpan CancelAfter = TimeSpan.FromMilliseconds(200);
    private static readonly TimeSpan Watchdog = TimeSpan.FromSeconds(20);

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task SqlServer_WhenCanceledWhileStatementRuns_StopsListAndStream()
    {
        var connection = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set DBACLIENTX_SQLSERVER_TEST_CONNECTION to a SQL Server database.");
        using var sql = new DBAClientX.SqlServer();
        const string query = "WAITFOR DELAY '00:01:00'; SELECT 1";

        await AssertStopsAsync(token => sql.QueryAsListAsync(connection!, query, record => record.GetInt32(0), cancellationToken: token));
        await AssertStopsAsync(async token =>
        {
            await foreach (var _ in sql.QueryStreamAsync(connection!, query, record => record.GetInt32(0), cancellationToken: token)) { }
        });
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSql_WhenCanceledWhileStatementRuns_StopsListAndStream()
    {
        var connection = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to a PostgreSQL database.");
        using var postgres = new DBAClientX.PostgreSql();
        const string query = "SELECT 1 FROM pg_sleep(60)";

        await AssertStopsAsync(token => postgres.QueryAsListAsync(connection!, query, record => record.GetInt32(0), cancellationToken: token));
        await AssertStopsAsync(async token =>
        {
            await foreach (var _ in postgres.QueryStreamAsync(connection!, query, record => record.GetInt32(0), cancellationToken: token)) { }
        });
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task MySql_WhenCanceledWhileStatementRuns_StopsListAndStream()
    {
        var connection = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set DBACLIENTX_MYSQL_TEST_CONNECTION to a MySQL database.");
        using var mySql = new DBAClientX.MySql();
        const string query = "SELECT SLEEP(60)";

        // KILL QUERY can make SLEEP() return 1 instead of failing, so MySQL may finish early without an exception.
        await AssertStopsAsync(token => mySql.QueryAsListAsync(connection!, query, record => record.GetInt64(0), cancellationToken: token), allowEarlyCompletion: true);
        await AssertStopsAsync(async token =>
        {
            await foreach (var _ in mySql.QueryStreamAsync(connection!, query, record => record.GetInt64(0), cancellationToken: token)) { }
        }, allowEarlyCompletion: true);
    }

    private static async Task AssertStopsAsync(Func<CancellationToken, Task> operation, bool allowEarlyCompletion = false)
    {
        using var cancellation = new CancellationTokenSource(CancelAfter);
        var clock = Stopwatch.StartNew();
        var running = Task.Run(() => operation(cancellation.Token));
        var finished = await Task.WhenAny(running, Task.Delay(Watchdog));
        Assert.True(ReferenceEquals(finished, running), "The canceled statement kept running past the watchdog.");
        if (!allowEarlyCompletion || !running.IsCompletedSuccessfully)
        {
            var exception = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => running);
            Assert.Equal(cancellation.Token, exception.CancellationToken);
        }

        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(5), $"The statement stopped after {clock.Elapsed.TotalMilliseconds:F0} ms.");
    }
}
