using System;
using System.Collections.Generic;
using DBAClientX;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Xunit;

namespace DbaClientX.Tests;

public class SqliteTests
{
    private static readonly FieldInfo TransactionField = typeof(DBAClientX.SQLite).GetField("_transaction", BindingFlags.Instance | BindingFlags.NonPublic)!;
    private static readonly FieldInfo TransactionConnectionField = typeof(DBAClientX.SQLite).GetField("_transactionConnection", BindingFlags.Instance | BindingFlags.NonPublic)!;

    [Fact]
    public void ExecuteNonQuery_CreatesAndReadsData()
    {
        var path = Path.GetTempFileName();
        try
        {
            using var sqlite = new DBAClientX.SQLite { ReturnType = ReturnType.DataTable };
            sqlite.ExecuteNonQuery(path, "CREATE TABLE t(id INTEGER);");
            sqlite.ExecuteNonQuery(path, "INSERT INTO t(id) VALUES (1);");
            var result = sqlite.Query(path, "SELECT id FROM t;");
            var table = Assert.IsType<DataTable>(result);
            Assert.Single(table.Rows);
            Assert.Equal(1L, table.Rows[0]["id"]);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_CreatesAndReadsData()
    {
        var path = Path.GetTempFileName();
        try
        {
            using var sqlite = new DBAClientX.SQLite { ReturnType = ReturnType.DataTable };
            await sqlite.ExecuteNonQueryAsync(path, "CREATE TABLE t(id INTEGER);");
            await sqlite.ExecuteNonQueryAsync(path, "INSERT INTO t(id) VALUES (1);");
            var result = await sqlite.QueryAsync(path, "SELECT id FROM t;");
            var table = Assert.IsType<DataTable>(result);
            Assert.Single(table.Rows);
            Assert.Equal(1L, table.Rows[0]["id"]);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public void Query_WithEmptySql_Throws()
    {
        using var sqlite = new DBAClientX.SQLite();

        Assert.Throws<ArgumentException>(() => sqlite.Query(":memory:", " "));
    }

    private class OutputDictionarySqlite : DBAClientX.SQLite
    {
        public override object? Query(string database, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, SqliteType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            using var command = new SqliteCommand();
            var dbTypes = DbTypeConverter.ConvertParameterTypes(parameterTypes, static () => new SqliteParameter(), static (p, t) => p.SqliteType = t);
            AddParameters(command, parameters, dbTypes, parameterDirections);
            foreach (SqliteParameter p in command.Parameters)
            {
                if (p.Direction != ParameterDirection.Input)
                {
                    p.Value = 5;
                }
            }
            UpdateOutputParameters(command, parameters);
            return null;
        }
    }

    [Fact]
    public void Query_WithOutputDirection_Throws()
    {
        using var sqlite = new OutputDictionarySqlite();
        var parameters = new Dictionary<string, object?> { ["@out"] = null };
        var directions = new Dictionary<string, ParameterDirection> { ["@out"] = ParameterDirection.Output };
        Assert.Throws<ArgumentException>(() => sqlite.Query(":memory:", "q", parameters, parameterDirections: directions));
    }

    private class OutputParameterSqlite : DBAClientX.SQLite
    {
        protected override int ExecuteNonQuery(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            if (parameters != null && parameterDirections != null && parameterDirections.TryGetValue("@out", out var dir) && dir != ParameterDirection.Input)
            {
                parameters["@out"] = 9;
            }
            return 1;
        }

        public override int ExecuteNonQuery(string database, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, SqliteType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            var dbTypes = DbTypeConverter.ConvertParameterTypes(parameterTypes, static () => new SqliteParameter(), static (p, t) => p.SqliteType = t);
            return ExecuteNonQuery(null!, null, query, parameters, dbTypes, parameterDirections);
        }
    }

    [Fact]
    public void ExecuteNonQuery_PopulatesOutputParameter()
    {
        using var sqlite = new OutputParameterSqlite();
        var parameters = new Dictionary<string, object?> { ["@out"] = null };
        var directions = new Dictionary<string, ParameterDirection> { ["@out"] = ParameterDirection.Output };

        sqlite.ExecuteNonQuery("db", "UPDATE t SET c=1", parameters, parameterDirections: directions);

        Assert.Equal(9, parameters["@out"]);
    }

    [Fact]
    public void Query_WithTransactionNotStarted_Throws()
    {
        var path = Path.GetTempFileName();
        try
        {
            using var sqlite = new DBAClientX.SQLite();
            var ex = Assert.Throws<DBAClientX.DbaQueryExecutionException>(() => sqlite.Query(path, "SELECT 1", useTransaction: true));
            Assert.IsType<DBAClientX.DbaTransactionException>(ex.InnerException);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public void Commit_WithoutTransaction_Throws()
    {
        using var sqlite = new DBAClientX.SQLite();
        Assert.Throws<DBAClientX.DbaTransactionException>(() => sqlite.Commit());
    }

    [Fact]
    public void Rollback_WithoutTransaction_Throws()
    {
        using var sqlite = new DBAClientX.SQLite();
        Assert.Throws<DBAClientX.DbaTransactionException>(() => sqlite.Rollback());
    }

    [Fact]
    public void Commit_PersistsChanges()
    {
        var path = Path.GetTempFileName();
        try
        {
            using var sqlite = new DBAClientX.SQLite { ReturnType = ReturnType.DataTable };
            sqlite.ExecuteNonQuery(path, "CREATE TABLE t(id INTEGER);");
            sqlite.BeginTransaction(path);
            sqlite.ExecuteNonQuery(path, "INSERT INTO t(id) VALUES (1);", useTransaction: true);
            sqlite.Commit();
            var result = sqlite.Query(path, "SELECT id FROM t;");
            var table = Assert.IsType<DataTable>(result);
            Assert.Single(table.Rows);
            Assert.Equal(1L, table.Rows[0]["id"]);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public void RunInTransaction_CommitsOnSuccess()
    {
        var path = Path.GetTempFileName();
        try
        {
            using var sqlite = new DBAClientX.SQLite { ReturnType = ReturnType.DataTable };
            sqlite.ExecuteNonQuery(path, "CREATE TABLE t(id INTEGER);");

            var result = sqlite.RunInTransaction(path, client =>
            {
                client.ExecuteNonQuery(path, "INSERT INTO t(id) VALUES (1);", useTransaction: true);
                return 42;
            });

            Assert.Equal(42, result);
            var table = Assert.IsType<DataTable>(sqlite.Query(path, "SELECT id FROM t;"));
            Assert.Single(table.Rows);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public void RunInTransaction_WhenOperationFails_RollsBackAndRethrows()
    {
        var path = Path.GetTempFileName();
        try
        {
            using var sqlite = new DBAClientX.SQLite { ReturnType = ReturnType.DataTable };
            sqlite.ExecuteNonQuery(path, "CREATE TABLE t(id INTEGER);");

            var ex = Assert.Throws<InvalidOperationException>(() =>
                sqlite.RunInTransaction(path, client =>
                {
                    client.ExecuteNonQuery(path, "INSERT INTO t(id) VALUES (1);", useTransaction: true);
                    throw new InvalidOperationException("boom");
                }));

            Assert.Equal("boom", ex.Message);
            var table = Assert.IsType<DataTable>(sqlite.Query(path, "SELECT id FROM t;"));
            Assert.Empty(table.Rows);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public void Rollback_DiscardsChanges()
    {
        var path = Path.GetTempFileName();
        try
        {
            using var sqlite = new DBAClientX.SQLite { ReturnType = ReturnType.DataTable };
            sqlite.ExecuteNonQuery(path, "CREATE TABLE t(id INTEGER);");
            sqlite.BeginTransaction(path);
            sqlite.ExecuteNonQuery(path, "INSERT INTO t(id) VALUES (1);", useTransaction: true);
            sqlite.Rollback();
            var result = sqlite.Query(path, "SELECT id FROM t;");
            var table = Assert.IsType<DataTable>(result);
            Assert.Equal(0, table.Rows.Count);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public void Dispose_EndsTransaction()
    {
        var sqlite = new DBAClientX.SQLite();
        sqlite.BeginTransaction(":memory:");
        Assert.True(sqlite.IsInTransaction);
        sqlite.Dispose();
        Assert.False(sqlite.IsInTransaction);
    }

    [Fact]
    public async Task DisposeAsync_EndsTransaction()
    {
        var sqlite = new DBAClientX.SQLite();
        await sqlite.BeginTransactionAsync(":memory:");
        Assert.True(sqlite.IsInTransaction);

        await sqlite.DisposeAsync();

        Assert.False(sqlite.IsInTransaction);
    }

    private sealed class DisposeTrackingSqlite : DBAClientX.SQLite
    {
        public int RollbackCalls { get; private set; }
        public int CleanupCalls { get; private set; }

        public void SeedActiveTransaction()
        {
            TransactionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(SqliteTransaction)));
            TransactionConnectionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(SqliteConnection)));
        }

        protected override void TryRollbackDbTransactionOnDispose(SqliteTransaction? transaction)
            => RollbackCalls++;

        protected override void DisposeTransactionResources(SqliteTransaction? transaction, SqliteConnection? connection)
            => CleanupCalls++;
    }

    [Fact]
    public void Dispose_WithActiveTransaction_RollsBackAndCleansUpOnce()
    {
        var sqlite = new DisposeTrackingSqlite();
        sqlite.SeedActiveTransaction();

        sqlite.Dispose();
        sqlite.Dispose();

        Assert.False(sqlite.IsInTransaction);
        Assert.Equal(1, sqlite.RollbackCalls);
        Assert.Equal(1, sqlite.CleanupCalls);
    }

    private sealed class ThrowingDisposeSqlite : DBAClientX.SQLite
    {
        public int RollbackCalls { get; private set; }
        public int CleanupCalls { get; private set; }

        public void SeedActiveTransaction()
        {
            TransactionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(SqliteTransaction)));
            TransactionConnectionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(SqliteConnection)));
        }

        protected override void TryRollbackDbTransactionOnDispose(SqliteTransaction? transaction)
            => RollbackCalls++;

        protected override void DisposeTransactionResources(SqliteTransaction? transaction, SqliteConnection? connection)
        {
            CleanupCalls++;
            throw new InvalidOperationException("cleanup failed");
        }
    }

    [Fact]
    public void Dispose_WhenCleanupThrows_ClearsTransactionState()
    {
        var sqlite = new ThrowingDisposeSqlite();
        sqlite.SeedActiveTransaction();

        var ex = Assert.Throws<InvalidOperationException>(() => sqlite.Dispose());

        Assert.Equal("cleanup failed", ex.Message);
        Assert.False(sqlite.IsInTransaction);
        Assert.Equal(1, sqlite.RollbackCalls);
        Assert.Equal(1, sqlite.CleanupCalls);
        sqlite.Dispose();
        Assert.Equal(1, sqlite.RollbackCalls);
        Assert.Equal(1, sqlite.CleanupCalls);
    }

    [Fact]
    public void DisposedClient_AllowsDeletingAndRecreatingDatabaseFile()
    {
        var path = Path.GetTempFileName();
        try
        {
            using (var sqlite = new DBAClientX.SQLite())
            {
                sqlite.ExecuteNonQuery(path, "CREATE TABLE t(id INTEGER);");
            }

            File.Delete(path);
            Assert.False(File.Exists(path));

            using var sqlite2 = new DBAClientX.SQLite();
            sqlite2.ExecuteNonQuery(path, "CREATE TABLE recreated(id INTEGER);");
            var count = sqlite2.ExecuteScalar(path, "SELECT COUNT(*) FROM sqlite_master WHERE name = 'recreated';");
            Assert.Equal(1L, count);
        }
        finally
        {
            Cleanup(path);
        }
    }

    private class PingSqlite : DBAClientX.SQLite
    {
        public bool ShouldFail { get; set; }

        public override object? ExecuteScalar(string database, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, SqliteType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            if (ShouldFail) throw new DBAClientX.DbaQueryExecutionException("fail", query, new Exception());
            return 1;
        }

        public override Task<object?> ExecuteScalarAsync(string database, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, SqliteType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            if (ShouldFail) throw new DBAClientX.DbaQueryExecutionException("fail", query, new Exception());
            return Task.FromResult<object?>(1);
        }
    }

    [Fact]
    public void Ping_ReturnsTrue_OnSuccess()
    {
        using var sqlite = new PingSqlite { ShouldFail = false };
        Assert.True(sqlite.Ping(":memory:"));
    }

    [Fact]
    public void Ping_ReturnsFalse_OnFailure()
    {
        using var sqlite = new PingSqlite { ShouldFail = true };
        Assert.False(sqlite.Ping(":memory:"));
    }

    [Fact]
    public async Task PingAsync_ReturnsTrue_OnSuccess()
    {
        using var sqlite = new PingSqlite { ShouldFail = false };
        Assert.True(await sqlite.PingAsync(":memory:"));
    }

    [Fact]
    public async Task PingAsync_ReturnsFalse_OnFailure()
    {
        using var sqlite = new PingSqlite { ShouldFail = true };
        Assert.False(await sqlite.PingAsync(":memory:"));
    }

    [Fact]
    public void ExecuteScalar_ReturnsValue()
    {
        var path = Path.GetTempFileName();
        try
        {
            using var sqlite = new DBAClientX.SQLite();
            sqlite.ExecuteNonQuery(path, "CREATE TABLE t(id INTEGER);");
            sqlite.ExecuteNonQuery(path, "INSERT INTO t(id) VALUES (1);");
            var result = sqlite.ExecuteScalar(path, "SELECT id FROM t;");
            Assert.Equal(1L, result);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task ExecuteScalarAsync_ReturnsValue()
    {
        var path = Path.GetTempFileName();
        try
        {
            using var sqlite = new DBAClientX.SQLite();
            sqlite.ExecuteNonQuery(path, "CREATE TABLE t(id INTEGER);");
            sqlite.ExecuteNonQuery(path, "INSERT INTO t(id) VALUES (1);");
            var result = await sqlite.ExecuteScalarAsync(path, "SELECT id FROM t;");
            Assert.Equal(1L, result);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task PrepareForShutdownAsync_WalDatabase_CheckpointsAndKeepsDatabaseHealthy()
    {
        var path = Path.GetTempFileName();
        string walPath = path + "-wal";

        try
        {
            using var sqlite = new DBAClientX.SQLite();
            await sqlite.ExecuteNonQueryAsync(path, "PRAGMA journal_mode=WAL;");
            await sqlite.ExecuteNonQueryAsync(path, "CREATE TABLE t(id INTEGER, payload TEXT);");
            await sqlite.ExecuteNonQueryAsync(path, "INSERT INTO t(id, payload) VALUES (1, $payload);", new Dictionary<string, object?>
            {
                ["$payload"] = new string('x', 4096)
            });

            await sqlite.PrepareForShutdownAsync(path, new SqliteShutdownMaintenanceOptions
            {
                CheckpointMode = SqliteCheckpointMode.Truncate,
                OptimizeAfterCheckpoint = true
            });

            if (File.Exists(walPath))
            {
                Assert.Equal(0, new FileInfo(walPath).Length);
            }

            await using var connection = new SqliteConnection(DBAClientX.SQLite.BuildConnectionString(path));
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = "PRAGMA integrity_check;";
            object? result = await command.ExecuteScalarAsync();
            Assert.Equal("ok", result?.ToString());
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task PrepareForShutdownAsync_WhenTransactionActive_Throws()
    {
        using var sqlite = new DBAClientX.SQLite();
        await sqlite.BeginTransactionAsync(":memory:");

        await Assert.ThrowsAsync<DBAClientX.DbaTransactionException>(async () =>
            await sqlite.PrepareForShutdownAsync(":memory:"));

        await sqlite.RollbackAsync();
    }

    [Fact]
    public async Task RunInTransactionAsync_CommitsOnSuccess()
    {
        var path = Path.GetTempFileName();
        try
        {
            using var sqlite = new DBAClientX.SQLite { ReturnType = ReturnType.DataTable };
            await sqlite.ExecuteNonQueryAsync(path, "CREATE TABLE t(id INTEGER);");

            var result = await sqlite.RunInTransactionAsync(path, async (client, token) =>
            {
                await client.ExecuteNonQueryAsync(path, "INSERT INTO t(id) VALUES (1);", useTransaction: true, cancellationToken: token);
                return 42;
            });

            Assert.Equal(42, result);
            var table = Assert.IsType<DataTable>(await sqlite.QueryAsync(path, "SELECT id FROM t;"));
            Assert.Single(table.Rows);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task RunInTransactionAsync_WhenOperationFails_RollsBackAndRethrows()
    {
        var path = Path.GetTempFileName();
        try
        {
            using var sqlite = new DBAClientX.SQLite { ReturnType = ReturnType.DataTable };
            await sqlite.ExecuteNonQueryAsync(path, "CREATE TABLE t(id INTEGER);");

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                sqlite.RunInTransactionAsync(path, async (client, token) =>
                {
                    await client.ExecuteNonQueryAsync(path, "INSERT INTO t(id) VALUES (1);", useTransaction: true, cancellationToken: token);
                    throw new InvalidOperationException("boom");
                }));

            Assert.Equal("boom", ex.Message);
            var table = Assert.IsType<DataTable>(await sqlite.QueryAsync(path, "SELECT id FROM t;"));
            Assert.Empty(table.Rows);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public void Query_WithMismatchedTransactionDatabase_Throws()
    {
        var path1 = Path.GetTempFileName();
        var path2 = Path.GetTempFileName();
        try
        {
            using var sqlite = new DBAClientX.SQLite();
            sqlite.BeginTransaction(path1);

            var ex = Assert.Throws<DBAClientX.DbaQueryExecutionException>(() => sqlite.Query(path2, "SELECT 1", useTransaction: true));

            Assert.IsType<DBAClientX.DbaTransactionException>(ex.InnerException);
            sqlite.Rollback();
        }
        finally
        {
            Cleanup(path1);
            Cleanup(path2);
        }
    }

    [Fact]
    public async Task QueryAsync_WithMismatchedTransactionDatabase_Throws()
    {
        var path1 = Path.GetTempFileName();
        var path2 = Path.GetTempFileName();
        try
        {
            using var sqlite = new DBAClientX.SQLite();
            await sqlite.BeginTransactionAsync(path1);

            var ex = await Assert.ThrowsAsync<DBAClientX.DbaQueryExecutionException>(() => sqlite.QueryAsync(path2, "SELECT 1", useTransaction: true));

            Assert.IsType<DBAClientX.DbaTransactionException>(ex.InnerException);
            await sqlite.RollbackAsync();
        }
        finally
        {
            Cleanup(path1);
            Cleanup(path2);
        }
    }

    private class DelaySqlite : DBAClientX.SQLite
    {
        private readonly TimeSpan _delay;
        private int _current;
        public int MaxConcurrency { get; private set; }

        public DelaySqlite(TimeSpan delay)
        {
            _delay = delay;
        }

        public override async Task<object?> QueryAsync(string database, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, SqliteType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            var running = Interlocked.Increment(ref _current);
            try
            {
                MaxConcurrency = Math.Max(MaxConcurrency, running);
                await Task.Delay(_delay, cancellationToken);
                return null;
            }
            finally
            {
                Interlocked.Decrement(ref _current);
            }
        }
    }

    [Fact]
    public async Task RunQueriesInParallel_RespectsMaxDegreeOfParallelism()
    {
        var queries = Enumerable.Repeat("SELECT 1", 3).ToArray();
        using var sqlite = new DelaySqlite(TimeSpan.FromMilliseconds(200));

        await sqlite.RunQueriesInParallel(queries, ":memory:", maxDegreeOfParallelism: 1);

        Assert.Equal(1, sqlite.MaxConcurrency);
    }

    [Fact]
    public async Task RunQueriesInParallel_UsesDefaultThrottling()
    {
        var queries = Enumerable.Repeat("SELECT 1", DBAClientX.SQLite.DefaultMaxParallelQueries * 4).ToArray();
        using var sqlite = new DelaySqlite(TimeSpan.FromMilliseconds(100));

        await sqlite.RunQueriesInParallel(queries, ":memory:");

        Assert.InRange(sqlite.MaxConcurrency, 1, DBAClientX.SQLite.DefaultMaxParallelQueries);
    }

    [Fact]
    public async Task RunQueriesInParallel_WithBlankQuery_ThrowsBeforeStartingWork()
    {
        using var sqlite = new DelaySqlite(TimeSpan.FromSeconds(5));

        var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
            sqlite.RunQueriesInParallel(new[] { "SELECT 1", " " }, ":memory:"));

        Assert.Equal("queries", exception.ParamName);
        Assert.Equal(0, sqlite.MaxConcurrency);
    }

    [Fact]
    public void BeginTransaction_UsesDefaultBusyTimeoutPragma()
    {
        using var sqlite = new DBAClientX.SQLite();
        sqlite.BeginTransaction(":memory:");
        using var command = GetTransactionConnection(sqlite).CreateCommand();
        command.CommandText = "PRAGMA busy_timeout;";
        var result = command.ExecuteScalar();
        Assert.Equal(DBAClientX.SQLite.DefaultBusyTimeoutMs, Assert.IsType<long>(result));
        sqlite.Rollback();
    }

    [Fact]
    public void BeginTransaction_UsesConfiguredBusyTimeoutPragma()
    {
        using var sqlite = new DBAClientX.SQLite
        {
            BusyTimeoutMs = 12000
        };

        sqlite.BeginTransaction(":memory:");
        using var command = GetTransactionConnection(sqlite).CreateCommand();
        command.CommandText = "PRAGMA busy_timeout;";
        var result = command.ExecuteScalar();
        Assert.Equal(12000L, result);
        sqlite.Rollback();
    }

    [Fact]
    public void Commit_WhenUnderlyingProviderThrows_ClearsTransactionState()
    {
        using var sqlite = new DBAClientX.SQLite();
        sqlite.BeginTransaction(":memory:");

        var connection = GetTransactionConnection(sqlite);
        connection.Dispose();

        Assert.ThrowsAny<Exception>(() => sqlite.Commit());
        Assert.False(sqlite.IsInTransaction);
        Assert.Throws<DBAClientX.DbaTransactionException>(() => sqlite.Commit());
    }

    [Fact]
    public void Rollback_WhenUnderlyingProviderThrows_ClearsTransactionState()
    {
        using var sqlite = new DBAClientX.SQLite();
        sqlite.BeginTransaction(":memory:");

        var connection = GetTransactionConnection(sqlite);
        connection.Dispose();

        Assert.ThrowsAny<Exception>(() => sqlite.Rollback());
        Assert.False(sqlite.IsInTransaction);
        Assert.Throws<DBAClientX.DbaTransactionException>(() => sqlite.Rollback());
    }

    [Fact]
    public async Task QueryAsync_CanBeCancelled()
    {
        using var sqlite = new DelaySqlite(TimeSpan.FromSeconds(5));
        using var cts = new CancellationTokenSource(100);
        await Assert.ThrowsAsync<TaskCanceledException>(async () =>
        {
            await sqlite.QueryAsync(":memory:", "q", cancellationToken: cts.Token);
        });
    }

    private static void Cleanup(string path)
    {
        TryDelete(path);
        TryDelete(path + "-wal");
        TryDelete(path + "-shm");
    }

    private static void TryDelete(string path)
    {
        if (!File.Exists(path))
        {
            return;
        }

        File.Delete(path);
    }

    private static SqliteConnection GetTransactionConnection(DBAClientX.SQLite sqlite)
    {
        var field = typeof(DBAClientX.SQLite).GetField("_transactionConnection", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(field);
        var connection = field.GetValue(sqlite) as SqliteConnection;
        Assert.NotNull(connection);
        return connection;
    }
}
