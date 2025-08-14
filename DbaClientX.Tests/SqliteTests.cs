using System;
using DBAClientX;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Xunit;

namespace DbaClientX.Tests;

public class SqliteTests
{
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
        SqliteConnection.ClearAllPools();
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }
}
