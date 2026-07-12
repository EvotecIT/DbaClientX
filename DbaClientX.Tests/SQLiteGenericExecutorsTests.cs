using System.Data;
using System.Diagnostics;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

[CollectionDefinition(nameof(SQLiteGenericExecutorsCollection), DisableParallelization = true)]
public sealed class SQLiteGenericExecutorsCollection;

[Collection(nameof(SQLiteGenericExecutorsCollection))]
public sealed class SQLiteGenericExecutorsTests
{
    private sealed class CaptureSQLite : DBAClientX.SQLite
    {
        public string? LastConnectionString { get; private set; }

        public string? LastDatabasePath { get; private set; }

        public bool AsyncDisposed { get; private set; }

        public bool SyncDisposed { get; private set; }

        public override Task<int> ExecuteNonQueryWithConnectionStringAsync(
            string connectionString,
            string query,
            IDictionary<string, object?>? parameters = null,
            bool useTransaction = false,
            CancellationToken cancellationToken = default,
            IDictionary<string, SqliteType>? parameterTypes = null,
            IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            LastConnectionString = connectionString;
            return Task.FromResult(7);
        }

        public override Task<int> ExecuteNonQueryAsync(
            string database,
            string query,
            IDictionary<string, object?>? parameters = null,
            bool useTransaction = false,
            CancellationToken cancellationToken = default,
            IDictionary<string, SqliteType>? parameterTypes = null,
            IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            LastDatabasePath = database;
            return Task.FromResult(8);
        }

        protected override async ValueTask DisposeAsyncCore()
        {
            AsyncDisposed = true;
            await base.DisposeAsyncCore().ConfigureAwait(false);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                SyncDisposed = true;
            }

            base.Dispose(disposing);
        }
    }

    [Fact]
    public async Task ExecuteSqlAsync_PreservesFullConnectionStringAndUsesAsyncDisposal()
    {
        var builder = new SqliteConnectionStringBuilder
        {
            DataSource = "data.db",
            Mode = SqliteOpenMode.ReadOnly,
            Cache = SqliteCacheMode.Shared,
            Pooling = false,
            DefaultTimeout = 17,
            ForeignKeys = true,
            RecursiveTriggers = true
        };
        var client = new CaptureSQLite();
        var originalFactory = DBAClientX.SQLiteGeneric.GenericExecutors.ClientFactory;
        DBAClientX.SQLiteGeneric.GenericExecutors.ClientFactory = () => client;

        try
        {
            var affected = await DBAClientX.SQLiteGeneric.GenericExecutors.ExecuteSqlAsync(
                builder.ConnectionString,
                "UPDATE items SET name = @name",
                new Dictionary<string, object?> { ["@name"] = "updated" });

            Assert.Equal(7, affected);
            Assert.Equal(builder.ConnectionString, client.LastConnectionString);
            Assert.Null(client.LastDatabasePath);
            Assert.True(client.AsyncDisposed);
            Assert.False(client.SyncDisposed);
        }
        finally
        {
            DBAClientX.SQLiteGeneric.GenericExecutors.ClientFactory = originalFactory;
        }
    }

    [Fact]
    public async Task ExecuteSqlAsync_PathContainingEquals_IsTreatedAsDatabasePath()
    {
        var path = Path.Join(Path.GetTempPath(), $"dbax={Guid.NewGuid():N}.db");
        try
        {
            await DBAClientX.SQLiteGeneric.GenericExecutors.ExecuteSqlAsync(
                path,
                "CREATE TABLE path_contract (id INTEGER NOT NULL);");

            using var sqlite = new DBAClientX.SQLite();
            var count = sqlite.ExecuteScalar(path, "SELECT COUNT(*) FROM sqlite_master WHERE name = 'path_contract';");
            Assert.Equal(1L, count);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            File.Delete(path);
        }
    }

    [Fact]
    public async Task ExecuteSqlAsync_FullReadOnlyConnectionStringPreservesProviderMode()
    {
        var path = Path.Join(Path.GetTempPath(), $"dbax-readonly-{Guid.NewGuid():N}.db");
        try
        {
            using (var sqlite = new DBAClientX.SQLite())
            {
                sqlite.ExecuteNonQuery(path, "CREATE TABLE mode_contract (id INTEGER NOT NULL);");
            }

            var connectionString = new SqliteConnectionStringBuilder
            {
                DataSource = path,
                Mode = SqliteOpenMode.ReadOnly,
                Pooling = false
            }.ConnectionString;

            await Assert.ThrowsAsync<DBAClientX.DbaQueryExecutionException>(() =>
                DBAClientX.SQLiteGeneric.GenericExecutors.ExecuteSqlAsync(
                    connectionString,
                    "INSERT INTO mode_contract (id) VALUES (1);"));

            using var verifier = new DBAClientX.SQLite();
            Assert.Equal(0L, verifier.ExecuteScalar(path, "SELECT COUNT(*) FROM mode_contract;"));
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            File.Delete(path);
        }
    }

    [Fact]
    public void ExecuteNonQueryWithConnectionString_FullReadOnlyConnectionStringPreservesProviderMode()
    {
        var path = Path.Join(Path.GetTempPath(), $"dbax-readonly-sync-{Guid.NewGuid():N}.db");
        try
        {
            using var sqlite = new DBAClientX.SQLite();
            sqlite.ExecuteNonQuery(path, "CREATE TABLE sync_mode_contract (id INTEGER NOT NULL);");
            var connectionString = new SqliteConnectionStringBuilder
            {
                DataSource = path,
                Mode = SqliteOpenMode.ReadOnly,
                Pooling = false
            }.ConnectionString;

            Assert.Throws<DBAClientX.DbaQueryExecutionException>(() =>
                sqlite.ExecuteNonQueryWithConnectionString(
                    connectionString,
                    "INSERT INTO sync_mode_contract (id) VALUES (1);"));

            Assert.Equal(0L, sqlite.ExecuteScalar(path, "SELECT COUNT(*) FROM sync_mode_contract;"));
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            File.Delete(path);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ExecuteNonQueryWithConnectionString_PreservesExplicitDefaultTimeout(bool asyncExecution)
    {
        var path = Path.Join(Path.GetTempPath(), $"dbax-timeout-{Guid.NewGuid():N}.db");
        try
        {
            using var blocker = new SqliteConnection($"Data Source={path};Pooling=False");
            blocker.Open();
            using (var setup = blocker.CreateCommand())
            {
                setup.CommandText = "CREATE TABLE timeout_contract (id INTEGER NOT NULL);";
                setup.ExecuteNonQuery();
            }

            using var transaction = blocker.BeginTransaction();
            using (var acquireWriteLock = blocker.CreateCommand())
            {
                acquireWriteLock.Transaction = transaction;
                acquireWriteLock.CommandText = "INSERT INTO timeout_contract (id) VALUES (1);";
                acquireWriteLock.ExecuteNonQuery();
            }

            using var sqlite = new DBAClientX.SQLite { BusyTimeoutMs = 10000 };
            var connectionString = new SqliteConnectionStringBuilder
            {
                DataSource = path,
                Pooling = false,
                DefaultTimeout = 1
            }.ConnectionString;
            var stopwatch = Stopwatch.StartNew();

            if (asyncExecution)
            {
                await Assert.ThrowsAsync<DBAClientX.DbaQueryExecutionException>(() =>
                    sqlite.ExecuteNonQueryWithConnectionStringAsync(
                        connectionString,
                        "INSERT INTO timeout_contract (id) VALUES (2);"));
            }
            else
            {
                Assert.Throws<DBAClientX.DbaQueryExecutionException>(() =>
                    sqlite.ExecuteNonQueryWithConnectionString(
                        connectionString,
                        "INSERT INTO timeout_contract (id) VALUES (2);"));
            }

            stopwatch.Stop();
            Assert.True(
                stopwatch.Elapsed < TimeSpan.FromSeconds(5),
                $"Explicit one-second timeout was overwritten; execution took {stopwatch.Elapsed}.");
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            File.Delete(path);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ExecuteNonQuery_WhenTransactionIsMissing_PreservesExecutionExceptionContract(bool asyncExecution)
    {
        using var sqlite = new DBAClientX.SQLite();
        DBAClientX.DbaQueryExecutionException exception;

        if (asyncExecution)
        {
            exception = await Assert.ThrowsAsync<DBAClientX.DbaQueryExecutionException>(() =>
                sqlite.ExecuteNonQueryWithConnectionStringAsync(
                    "Data Source=:memory:;Pooling=False",
                    "UPDATE items SET id = 1;",
                    useTransaction: true));
        }
        else
        {
            exception = Assert.Throws<DBAClientX.DbaQueryExecutionException>(() =>
                sqlite.ExecuteNonQuery(
                    ":memory:",
                    "UPDATE items SET id = 1;",
                    useTransaction: true));
        }

        Assert.IsType<DBAClientX.DbaTransactionException>(exception.InnerException);
    }
}
