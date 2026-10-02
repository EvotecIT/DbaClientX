using DBAClientX;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

public sealed class CommandReplaySafetyTests : IDisposable
{
    private readonly string _database = Path.Combine(Path.GetTempPath(), "dbx-replay-" + Guid.NewGuid().ToString("N") + ".db");
    private readonly SQLite _sqlite;
    private int _calls;

    public CommandReplaySafetyTests()
    {
        _sqlite = new ControlledFaultSQLite { MaxRetryAttempts = 3, RetryDelay = TimeSpan.Zero };
        _sqlite.ConfigureConnection = connection => connection.CreateFunction<long>("transient_failure", () =>
        {
            if (++_calls < 3) throw new SqliteException("I/O failure after an earlier statement committed", 10);
            return 42;
        });
        _sqlite.ExecuteNonQuery(_database, "CREATE TABLE writes (id INTEGER PRIMARY KEY)");
    }

    [Theory]
    [InlineData("scalar")]
    [InlineData("scalar-async")]
    [InlineData("query")]
    [InlineData("query-async")]
    [InlineData("mapped")]
    [InlineData("reader")]
    [InlineData("stream")]
    [InlineData("session")]
    [InlineData("prepared")]
    public async Task ResultReturningBatch_WhenAnEarlierWriteCommitted_DoesNotReplayByDefault(string operation)
    {
        const string sql = "INSERT INTO writes DEFAULT VALUES; SELECT transient_failure()";

        var failure = await Record.ExceptionAsync(() => ExecuteAsync(operation, sql));
        // Streaming retains its existing provider-exception contract; buffered/session APIs wrap it.
        if (operation == "stream") Assert.IsType<SqliteException>(failure);
        else Assert.IsType<DbaQueryExecutionException>(failure);

        Assert.Equal(1, _calls);
        Assert.Equal(1L, _sqlite.ExecuteScalar(_database, "SELECT count(*) FROM writes"));
    }

    [Fact]
    public async Task ReplaySafeQuery_WhenExplicitlyEnabled_RetriesTransientFailures()
    {
        _sqlite.CommandRetryMode = CommandRetryMode.ReplaySafe;

        Assert.Equal(42L, await _sqlite.ExecuteScalarAsync(_database, "SELECT transient_failure()"));

        Assert.Equal(3, _calls);
        Assert.Equal(0L, _sqlite.ExecuteScalar(_database, "SELECT count(*) FROM writes"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TransactionCommand_EvenWithReplayEnabled_ExecutesOnce(bool asynchronous)
    {
        _sqlite.CommandRetryMode = CommandRetryMode.ReplaySafe;
        _sqlite.RetryNonQueryOperations = true;
        // Classify an injected SQLITE_ERROR as transient at the real provider boundary. Native SQLITE_BUSY
        // would make Microsoft.Data.Sqlite invoke its own busy handler, obscuring DbaClientX attempt counts.
        _sqlite.ConfigureConnection = connection => connection.CreateFunction<long>("busy", () =>
        {
            _calls++;
            throw new SqliteException("injected transient failure", 1);
        });

        if (asynchronous)
        {
            await using var session = await _sqlite.OpenSessionAsync(_database);
            await Assert.ThrowsAsync<DbaQueryExecutionException>(() => session.RunInTransactionAsync(async (transaction, token) =>
            {
                await transaction.ExecuteNonQueryAsync("INSERT INTO writes DEFAULT VALUES", cancellationToken: token);
                using var prepared = transaction.Prepare("SELECT busy()");
                await prepared.ExecuteScalarAsync(Array.Empty<object?>(), token);
            }));
        }
        else
        {
            using var session = _sqlite.OpenSession(_database);
            Assert.Throws<DbaQueryExecutionException>(() => session.RunInTransaction(transaction =>
            {
                transaction.ExecuteNonQuery("INSERT INTO writes DEFAULT VALUES");
                transaction.ExecuteScalar("SELECT busy()");
            }));
        }

        Assert.Equal(1, _calls);
        Assert.Equal(0L, _sqlite.ExecuteScalar(_database, "SELECT count(*) FROM writes"));
    }

    private async Task ExecuteAsync(string operation, string sql)
    {
        switch (operation)
        {
            case "scalar": _sqlite.ExecuteScalar(_database, sql); break;
            case "scalar-async": await _sqlite.ExecuteScalarAsync(_database, sql); break;
            case "query": _sqlite.Query(_database, sql); break;
            case "query-async": await _sqlite.QueryAsync(_database, sql); break;
            case "mapped": await _sqlite.QueryAsListAsync(_database, sql, row => row.GetInt64(0)); break;
            case "reader": await using (var reader = await _sqlite.QueryReaderAsync(_database, sql)) { } break;
            case "stream": await foreach (var row in _sqlite.QueryStreamAsync(_database, sql)) { } break;
            case "session":
                await using (var session = await _sqlite.OpenSessionAsync(_database))
                    await session.ExecuteScalarAsync(sql);
                break;
            case "prepared":
                using (var session = _sqlite.OpenSession(_database))
                using (var command = session.Prepare(sql))
                    await command.ExecuteScalarAsync(Array.Empty<object?>());
                break;
            default: throw new ArgumentOutOfRangeException(nameof(operation));
        }
    }

    public void Dispose()
    {
        _sqlite.Dispose();
        foreach (string path in new[] { _database, _database + "-wal", _database + "-shm" })
            if (File.Exists(path)) File.Delete(path);
    }

    private sealed class ControlledFaultSQLite : SQLite
    {
        protected override bool IsTransient(Exception exception)
            => exception is SqliteException { SqliteErrorCode: 1 } || base.IsTransient(exception);
    }
}
