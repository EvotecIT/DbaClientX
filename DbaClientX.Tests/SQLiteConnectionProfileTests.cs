using System.Data;
using DBAClientX;
using DBAClientX.DataMovement;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

[Collection(SqlitePoolCleanupCollection.Name)]
public sealed class SQLiteConnectionProfileTests : IDisposable
{
    private readonly string _database = Path.Combine(Path.GetTempPath(), "dbx-profile-" + Guid.NewGuid().ToString("N") + ".db");

    [Fact]
    public async Task Profile_AppliesToCommandsSessionsAndTransactionsWithSnapshotOwnership()
    {
        var options = new SQLiteConnectionOptions { BusyTimeoutMs = 4321, CacheSize = -876, Pooling = false };
        using var client = new SQLite { ConnectionOptions = options };
        options.BusyTimeoutMs = 1;
        client.ConnectionOptions!.CacheSize = -1;
        Assert.Equal(4321L, client.ExecuteScalar(_database, "PRAGMA busy_timeout"));
        Assert.Equal(-876L, await client.ExecuteScalarAsync(_database, "PRAGMA cache_size"));
        Assert.Equal(1L, await client.ExecuteScalarAsync(_database, "PRAGMA foreign_keys"));
        Assert.Equal("wal", client.ExecuteScalar(_database, "PRAGMA journal_mode"));
        using (var session = client.OpenSession(_database))
        {
            Assert.Equal(4321L, session.ExecuteScalar("PRAGMA busy_timeout"));
            Assert.Equal(1L, session.ExecuteScalar("PRAGMA synchronous"));
        }
        await using (var session = await client.OpenSessionAsync(_database))
            Assert.Equal(-876L, await session.ExecuteScalarAsync("PRAGMA cache_size"));

        client.BeginTransaction(_database);
        try { Assert.Equal(1L, client.ExecuteScalar(_database, "PRAGMA foreign_keys", useTransaction: true)); }
        finally { client.Rollback(); }
    }

    [Fact]
    public async Task ProfileBusyTimeout_ExplicitMethodArgumentWinsOverProfileAndConnectionString()
    {
        using var client = new SQLite { ConnectionOptions = new SQLiteConnectionOptions { BusyTimeoutMs = 4321 } };
        Assert.Equal(4321L, client.ExecuteScalarWithConnectionString(SQLite.BuildConnectionString(_database) + ";Default Timeout=9", "PRAGMA busy_timeout"));
        var values = await client.QueryReadOnlyAsListAsync(_database, "PRAGMA busy_timeout", row => row.GetInt64(0), busyTimeoutMs: 123);
        Assert.Equal(123L, Assert.Single(values));
    }

    [Fact]
    public void ManagedExplicitOptions_OverrideProfileWithoutApplyingItTwice()
    {
        using var client = new SQLite { ConnectionOptions = new SQLiteConnectionOptions { CacheSize = -876, Pooling = true, BusyTimeoutMs = 4321 } };
        using var connection = client.OpenDbConnection(_database, new SQLiteConnectionOptions { CacheSize = -321, Pooling = false });
        using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA cache_size";
        Assert.Equal(-321L, command.ExecuteScalar());
        command.CommandText = "PRAGMA busy_timeout";
        Assert.Equal((long)SQLite.DefaultBusyTimeoutMs, command.ExecuteScalar());
        Assert.False(new SqliteConnectionStringBuilder(connection.ConnectionString).Pooling);
    }

    [Fact]
    public async Task ReadOnlyProfile_PreservesJournalModeAndRejectsWritesFromConnectionStrings()
    {
        using (var seed = new SQLite()) seed.ExecuteNonQuery(_database, "CREATE TABLE Items (Id INTEGER PRIMARY KEY)");
        using var client = new SQLite { ConnectionOptions = new SQLiteConnectionOptions { ReadOnly = true, Pooling = false } };
        Assert.Equal("delete", client.ExecuteScalar(_database, "PRAGMA journal_mode"));
        Assert.Equal(0L, await client.ExecuteScalarAsync(_database, "SELECT count(*) FROM Items"));
        var exception = Assert.Throws<DbaQueryExecutionException>(() => client.ExecuteNonQueryWithConnectionString(
            SQLite.BuildConnectionString(_database), "INSERT INTO Items VALUES (1)"));
        Assert.Equal(8, exception.ProviderErrorCode);
    }

    [Fact]
    public async Task CopyAdapter_RegistersUnicodeForReadsWritesPreflightAndAtomicCheckpoints()
    {
        using var client = new SQLite { ConfigureConnection = SQLiteUnicodeText.Register };
        client.ExecuteNonQuery(_database, "CREATE TABLE Items (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL); " +
            "CREATE INDEX IX_Name ON Items(dbx_lower(Name)); " +
            "CREATE VIEW Folded AS SELECT Id, dbx_lower(Name) AS Name FROM Items");
        var adapter = new SQLiteTableCopyAdapter(_database)
        {
            ConnectionOptions = new SQLiteConnectionOptions { BusyTimeoutMs = 1234, Pooling = false },
            ConfigureConnection = SQLiteUnicodeText.Register
        };
        var definition = new DbaTableCopyDefinition("Folded", "Items", new[] { "Id" }) { UseKeysetPagination = true };
        using var page = new DataTable();
        page.Columns.Add("Id", typeof(long));
        page.Columns.Add("Name", typeof(string));
        page.Rows.Add(1L, "ŻÓŁĆ");
        await adapter.WritePageAsync(definition, page, new DbaTableCopyOptions());
        Assert.Equal(1L, await adapter.CountRowsAsync(definition));
        using (var readPage = await adapter.ReadPageAsync(new DbaTableCopyPageRequest(definition, null, 10)))
            Assert.Equal("żółć", Assert.Single(readPage.Data.Rows.Cast<DataRow>())["Name"]);

        await using (var preflight = await adapter.OpenSchemaPreflightSessionAsync(definition, page,
            new DbaTableCopyOptions { ClearDestination = true }, CancellationToken.None)) { }
        Assert.Equal(1L, await adapter.CountRowsAsync(definition));

        var checkpoint = new DbaTableCopyCheckpoint
        {
            CopyId = "unicode-profile", DefinitionFingerprint = new string('a', 64), SourceRows = 1,
            SourceContentHash = new string('b', 64), CopiedContentHash = new string('c', 64)
        };
        await adapter.InitializeCheckpointAsync(definition, checkpoint, clearDestination: true);
        await adapter.CommitPageAsync(definition, page, new DbaTableCopyOptions(), checkpoint,
            checkpoint with { CopiedRows = 1 });
        Assert.Equal(1, (await adapter.ReadCheckpointAsync(definition))!.CopiedRows);
        Assert.Equal(1L, await adapter.CountRowsAsync(definition));
    }

    [Fact]
    public void ReadOnlyProfile_RejectsMemoryTargetsRatherThanOpeningWritableMemory()
    {
        using var client = new SQLite { ConnectionOptions = new SQLiteConnectionOptions { ReadOnly = true } };
        Assert.Throws<ArgumentException>(() => client.OpenDbConnection(":memory:"));
    }

    [Fact]
    public void ExplicitClientTimeout_AppliesToProviderCommandsAndResetRestoresConnectionStringDefault()
    {
        SqliteConnection? connection = null;
        using var client = new SQLite
        {
            ConfigureConnection = value => connection = value
        };
        string connectionString = SQLite.BuildConnectionString(_database) + ";Default Timeout=9";
        client.ExecuteScalarWithConnectionString(connectionString, "SELECT 1");
        Assert.Equal(9, connection!.DefaultTimeout);
        client.CommandTimeout = 1;
        client.ExecuteScalarWithConnectionString(connectionString, "SELECT 1");
        Assert.Equal(1, connection!.DefaultTimeout);
        client.CommandTimeout = 0;
        client.ExecuteScalarWithConnectionString(connectionString, "SELECT 1");
        Assert.Equal(0, connection!.DefaultTimeout);
        client.ResetCommandTimeout();
        client.ExecuteScalarWithConnectionString(connectionString, "SELECT 1");
        Assert.Equal(9, connection!.DefaultTimeout);
    }

    [Theory]
    [InlineData("SyncSession")]
    [InlineData("AsyncSession")]
    [InlineData("Transaction")]
    [InlineData("Managed")]
    public async Task ResetTimeout_RestoresDefaultsForCommandsOnRetainedConnections(string path)
    {
        SqliteConnection? connection = null;
        using var client = new SQLite { CommandTimeout = 0, ConfigureConnection = value => connection = value };
        IDisposable? session = null;
        IAsyncDisposable? asyncSession = null;
        try
        {
            if (path == "SyncSession") session = client.OpenSession(_database);
            else if (path == "AsyncSession") asyncSession = await client.OpenSessionAsync(_database);
            else if (path == "Transaction") client.BeginTransaction(_database);
            else session = client.OpenDbConnection(_database);
            Assert.NotNull(connection);
            using (var command = connection.CreateCommand()) Assert.Equal(0, command.CommandTimeout);
            client.ResetCommandTimeout();
            using (var command = connection.CreateCommand()) Assert.Equal(30, command.CommandTimeout);
            client.CommandTimeout = 4;
            using (var command = connection.CreateCommand()) Assert.Equal(4, command.CommandTimeout);
            client.ResetCommandTimeout();
            using (var command = connection.CreateCommand()) Assert.Equal(30, command.CommandTimeout);
        }
        finally
        {
            if (path == "Transaction") client.Rollback();
            session?.Dispose();
            if (asyncSession != null) await asyncSession.DisposeAsync();
        }
    }

    [Theory]
    [InlineData("SyncSession", false)]
    [InlineData("AsyncSession", false)]
    [InlineData("Transaction", false)]
    [InlineData("Managed", false)]
    [InlineData("SyncSession", true)]
    [InlineData("AsyncSession", true)]
    [InlineData("Transaction", true)]
    [InlineData("Managed", true)]
    public async Task CallbackDefault_IsPreservedUnlessClientTimeoutIsExplicitAndRestoredOnReset(string path, bool explicitTimeout)
    {
        SqliteConnection? connection = null;
        using var client = new SQLite { ConfigureConnection = value => { value.DefaultTimeout = 7; connection = value; } };
        if (explicitTimeout) client.CommandTimeout = 0;
        IDisposable? session = null;
        IAsyncDisposable? asyncSession = null;
        try
        {
            if (path == "SyncSession") session = client.OpenSession(_database);
            else if (path == "AsyncSession") asyncSession = await client.OpenSessionAsync(_database);
            else if (path == "Transaction") client.BeginTransaction(_database);
            else session = client.OpenDbConnection(_database);
            Assert.NotNull(connection);
            Assert.Equal(explicitTimeout ? 0 : 7, connection.DefaultTimeout);
            client.CommandTimeout = 4;
            using (var command = connection.CreateCommand()) Assert.Equal(4, command.CommandTimeout);
            client.ResetCommandTimeout();
            using (var command = connection.CreateCommand()) Assert.Equal(7, command.CommandTimeout);
        }
        finally
        {
            if (path == "Transaction") client.Rollback();
            session?.Dispose();
            if (asyncSession != null) await asyncSession.DisposeAsync();
        }
    }

    [Fact]
    public async Task CopyAdapter_OrdinaryPageWrite_UsesAdapterCommandTimeout()
    {
        using var locker = new SQLite { BusyTimeoutMs = 1 };
        locker.ExecuteNonQuery(_database, "CREATE TABLE Items (Id INTEGER PRIMARY KEY)");
        locker.BeginTransaction(_database);
        try
        {
            locker.ExecuteNonQuery(_database, "INSERT INTO Items VALUES (1)", useTransaction: true);
            var adapter = new SQLiteTableCopyAdapter(_database)
            {
                CommandTimeout = 1,
                ConnectionOptions = new SQLiteConnectionOptions
                {
                    BusyTimeoutMs = 1, EnableWriteAheadLogging = false, UseNormalSynchronousMode = false
                }
            };
            using var page = new DataTable();
            page.Columns.Add("Id", typeof(long));
            page.Rows.Add(2L);
            using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            // The provider waits synchronously. A command timeout should report BUSY before the watchdog token.
            var exception = await Assert.ThrowsAsync<DbaQueryExecutionException>(() => Task.Run(() => adapter.WritePageAsync(
                new DbaTableCopyDefinition("Items", "Items", new[] { "Id" }), page, new DbaTableCopyOptions(), cancellation.Token)));
            Assert.True(exception.ProviderErrorCode is 5 or 6);
            Assert.False(cancellation.IsCancellationRequested);
        }
        finally { locker.Rollback(); }
    }

    [Fact]
    public async Task CopyAdapter_ProfileRunsBeforeEveryReadSessionCommandAndFailedCallbackReleasesConnection()
    {
        using (var seed = new SQLite()) seed.ExecuteNonQuery(_database, "CREATE TABLE Items (Id INTEGER PRIMARY KEY)");
        int initialized = 0;
        var adapter = new SQLiteTableCopyAdapter(_database)
        {
            ReadConsistency = DbaTableCopyReadConsistency.Snapshot,
            ConnectionOptions = new SQLiteConnectionOptions { ReadOnly = true, CacheSize = -876, BusyTimeoutMs = 1234 },
            ConfigureConnection = connection =>
            {
                using var command = connection.CreateCommand();
                command.CommandText = "PRAGMA cache_size";
                Assert.Equal(-876L, command.ExecuteScalar());
                command.CommandText = "PRAGMA busy_timeout";
                Assert.Equal(1234L, command.ExecuteScalar());
                Interlocked.Increment(ref initialized);
            }
        };
        var definition = new DbaTableCopyDefinition("Items", "Items", new[] { "Id" });
        using (await adapter.OpenReadSessionAsync())
        {
            Assert.Equal(0L, await adapter.CountRowsAsync(definition));
            Assert.Equal(0L, await adapter.CountRowsAsync(definition));
        }
        Assert.Equal(1, initialized);
        adapter.ConfigureConnection = _ => throw new InvalidOperationException("registration failed");
        await Assert.ThrowsAsync<InvalidOperationException>(() => adapter.OpenReadSessionAsync());
        adapter.ConfigureConnection = null;
        using (await adapter.OpenReadSessionAsync()) Assert.Equal(0L, await adapter.CountRowsAsync(definition));
    }

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        foreach (var file in new[] { _database, _database + "-wal", _database + "-shm" })
            if (File.Exists(file)) File.Delete(file);
    }
}
