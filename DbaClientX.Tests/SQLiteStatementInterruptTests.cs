using System.Data;
using System.Diagnostics;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

/// <summary>
/// A canceled token stops a SQLite statement that is running, instead of letting it run to completion.
/// </summary>
[Collection(SqlitePoolCleanupCollection.Name)]
public sealed class SQLiteStatementInterruptTests : IDisposable
{
    // Counts to a billion: minutes of work, so only an interrupt ends it within the watchdog.
    private const string EndlessQuery =
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM (SELECT x FROM c LIMIT 1000000000)";

    private static readonly TimeSpan CancelAfter = TimeSpan.FromMilliseconds(200);
    private static readonly TimeSpan Watchdog = TimeSpan.FromSeconds(15);

    private readonly string _database = Path.Combine(Path.GetTempPath(), "dbx-interrupt-" + Guid.NewGuid().ToString("N") + ".db");

    public SQLiteStatementInterruptTests()
    {
        using var sqlite = new DBAClientX.SQLite();
        sqlite.ExecuteNonQuery(_database, "CREATE TABLE Numbers (Value INTEGER NOT NULL)");
    }

    public static TheoryData<string> Operations => new()
    {
        "QueryAsync",
        "QueryWithConnectionStringAsync",
        "QueryAsListAsync",
        "QueryReadOnlyAsync",
        "QueryReadOnlyAsListAsync",
        "ExecuteScalarAsync",
        "QueryStreamAsync",
        "QueryStreamWithConnectionStringAsync",
        "QueryStreamAsyncMapped",
        "QueryStreamWithConnectionStringAsyncMapped",
        "QueryReaderAsync",
        "QueryReadOnlyStreamAsync"
    };

    [Theory]
    [MemberData(nameof(Operations))]
    public async Task Operation_WhenCanceledWhileStatementRuns_InterruptsTheStatement(string operation)
    {
        using var sqlite = new DBAClientX.SQLite();
        using var cancellation = new CancellationTokenSource(CancelAfter);

        await AssertCanceledWithinWatchdogAsync(() => RunAsync(sqlite, operation, EndlessQuery, cancellation.Token), cancellation.Token);
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_WhenCanceledWhileStatementRuns_RollsTheStatementBack()
    {
        using var sqlite = new DBAClientX.SQLite();
        using var cancellation = new CancellationTokenSource(CancelAfter);
        var insert = "INSERT INTO Numbers (Value) " + EndlessQuery.Replace("SELECT count(*) FROM", "SELECT x FROM", StringComparison.Ordinal);

        await AssertCanceledWithinWatchdogAsync(() => sqlite.ExecuteNonQueryAsync(_database, insert, cancellationToken: cancellation.Token), cancellation.Token);

        Assert.Equal(0L, Convert.ToInt64(sqlite.ExecuteScalar(_database, "SELECT count(*) FROM Numbers")));
    }

    [Fact]
    public async Task QueryWithConnectionStringAsync_AfterAnInterruptedQuery_PooledConnectionRunsTheNextQuery()
    {
        // One pooled connection serves both queries: the interrupt of the first must not stop the second.
        var connectionString = "Data Source=" + _database + ";Pooling=True";
        using var sqlite = new DBAClientX.SQLite { ReturnType = DBAClientX.ReturnType.DataTable };
        using (var cancellation = new CancellationTokenSource(CancelAfter))
        {
            await AssertCanceledWithinWatchdogAsync(() => sqlite.QueryWithConnectionStringAsync(connectionString, EndlessQuery, cancellationToken: cancellation.Token), cancellation.Token);
        }

        using var unused = new CancellationTokenSource();
        var table = Assert.IsType<DataTable>(await sqlite.QueryWithConnectionStringAsync(
            connectionString,
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 200000) SELECT count(*) FROM c",
            cancellationToken: unused.Token));

        Assert.Equal(200000L, Convert.ToInt64(table.Rows[0][0]));
    }

    [Fact]
    public async Task QueryReaderAsync_WhenOpeningTokenIsCanceledWhileRowsAreRead_InterruptsTheStatement()
    {
        using var sqlite = new DBAClientX.SQLite();
        using var cancellation = new CancellationTokenSource();
        // Rows arrive at once, then the scan runs for minutes before the next match.
        const string sparse = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 1000000000) SELECT x FROM c WHERE x = 1 OR x = 1000000000";
        await using var reader = await sqlite.QueryReaderAsync(_database, sparse, cancellationToken: cancellation.Token);
        Assert.True(await reader.ReadAsync(CancellationToken.None));
        cancellation.CancelAfter(CancelAfter);

        var elapsed = await AssertCanceledWithinWatchdogAsync(() => reader.ReadAsync(CancellationToken.None), cancellation.Token);

        Assert.True(elapsed < TimeSpan.FromSeconds(5), $"The reader stopped after {elapsed.TotalMilliseconds:F0} ms.");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task TableCopyReadPageAsync_WhenCanceledWhileStatementRuns_StopsWithOperationCanceled(bool keyset)
    {
        CreateSlowView();
        var adapter = new DBAClientX.SQLiteTableCopyAdapter(_database);
        var definition = new DBAClientX.DataMovement.DbaTableCopyDefinition("SlowRows", "Target", new[] { "Id" }) { UseKeysetPagination = keyset };
        using var cancellation = new CancellationTokenSource(CancelAfter);

        await AssertCanceledWithinWatchdogAsync(
            () => adapter.ReadPageAsync(new DBAClientX.DataMovement.DbaTableCopyPageRequest(definition, null, 100), cancellation.Token),
            cancellation.Token);
    }

    [Fact]
    public async Task TableCopyCountRowsAsync_WhenCanceledWhileStatementRuns_StopsWithOperationCanceled()
    {
        CreateSlowView();
        var adapter = new DBAClientX.SQLiteTableCopyAdapter(_database);
        var definition = new DBAClientX.DataMovement.DbaTableCopyDefinition("SlowRows", "Target", new[] { "Id" });
        using var cancellation = new CancellationTokenSource(CancelAfter);

        await AssertCanceledWithinWatchdogAsync(() => adapter.CountRowsAsync(definition, cancellation.Token), cancellation.Token);
    }

    private void CreateSlowView()
    {
        using var sqlite = new DBAClientX.SQLite();
        // Scans a billion generated rows and returns none, in constant memory.
        sqlite.ExecuteNonQuery(_database, "CREATE VIEW SlowRows AS WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 1000000000) SELECT x AS Id FROM c WHERE x < 0");
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_InsideClientTransaction_CancellationDoesNotRollBackTheTransaction()
    {
        // An interrupted write rolls back the whole explicit transaction, so the client's shared transaction connection
        // is never interrupted: the caller's earlier work in the transaction must still commit.
        using var sqlite = new DBAClientX.SQLite();
        sqlite.BeginTransaction(_database);
        try
        {
            await sqlite.ExecuteNonQueryAsync(_database, "INSERT INTO Numbers (Value) VALUES (-1)", useTransaction: true);
            using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
            var insert = "INSERT INTO Numbers (Value) " + EndlessQuery
                .Replace("SELECT count(*) FROM", "SELECT x FROM", StringComparison.Ordinal)
                .Replace("LIMIT 1000000000", "LIMIT 4000000", StringComparison.Ordinal);
            try
            {
                await sqlite.ExecuteNonQueryAsync(_database, insert, useTransaction: true, cancellationToken: cancellation.Token);
            }
            catch (OperationCanceledException)
            {
            }

            sqlite.Commit();
        }
        finally
        {
            if (sqlite.IsInTransaction)
            {
                sqlite.Rollback();
            }
        }

        Assert.Equal(1L, Convert.ToInt64(sqlite.ExecuteScalar(_database, "SELECT count(*) FROM Numbers WHERE Value = -1")));
    }

    private async Task RunAsync(DBAClientX.SQLite sqlite, string operation, string query, CancellationToken token)
    {
        // Microsoft.Data.Sqlite pools connection-string connections by default: an interrupt must not outlive its statement.
        var connectionString = "Data Source=" + _database;
        switch (operation)
        {
            case "QueryAsync":
                await sqlite.QueryAsync(_database, query, cancellationToken: token);
                break;
            case "QueryWithConnectionStringAsync":
                await sqlite.QueryWithConnectionStringAsync(connectionString, query, cancellationToken: token);
                break;
            case "QueryAsListAsync":
                await sqlite.QueryAsListAsync(_database, query, record => record.GetInt64(0), cancellationToken: token);
                break;
            case "QueryReadOnlyAsync":
                await sqlite.QueryReadOnlyAsync(_database, query, cancellationToken: token);
                break;
            case "QueryReadOnlyAsListAsync":
                await sqlite.QueryReadOnlyAsListAsync(_database, query, reader => reader.GetInt64(0), cancellationToken: token);
                break;
            case "ExecuteScalarAsync":
                await sqlite.ExecuteScalarAsync(_database, query, cancellationToken: token);
                break;
            case "QueryStreamAsync":
                await foreach (var _ in sqlite.QueryStreamAsync(_database, query, cancellationToken: token)) { }
                break;
            case "QueryStreamWithConnectionStringAsync":
                await foreach (var _ in sqlite.QueryStreamWithConnectionStringAsync(connectionString, query, cancellationToken: token)) { }
                break;
            case "QueryStreamAsyncMapped":
                await foreach (var _ in sqlite.QueryStreamAsync(_database, query, record => record.GetInt64(0), cancellationToken: token)) { }
                break;
            case "QueryStreamWithConnectionStringAsyncMapped":
                await foreach (var _ in sqlite.QueryStreamWithConnectionStringAsync(connectionString, query, record => record.GetInt64(0), cancellationToken: token)) { }
                break;
            case "QueryReadOnlyStreamAsync":
                await foreach (var _ in sqlite.QueryReadOnlyStreamAsync(_database, query, record => record.GetInt64(0), cancellationToken: token)) { }
                break;
            case "QueryReaderAsync":
                await using (var reader = await sqlite.QueryReaderAsync(_database, query, cancellationToken: token)) { }
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(operation), operation, null);
        }
    }

    private static async Task<TimeSpan> AssertCanceledWithinWatchdogAsync(Func<Task> operation, CancellationToken token)
    {
        var clock = Stopwatch.StartNew();
        // Microsoft.Data.Sqlite runs statements synchronously on the calling thread; start the operation elsewhere so the watchdog can fire.
        var running = Task.Run(operation);
        var finished = await Task.WhenAny(running, Task.Delay(Watchdog));
        Assert.True(ReferenceEquals(finished, running), "The canceled statement kept running past the watchdog.");
        var exception = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => running);
        Assert.Equal(token, exception.CancellationToken);
        return clock.Elapsed;
    }

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        try
        {
            File.Delete(_database);
        }
        catch (IOException)
        {
        }
    }
}
