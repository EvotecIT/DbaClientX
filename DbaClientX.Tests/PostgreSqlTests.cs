using System;
using Npgsql;
using NpgsqlTypes;
using System.Data.Common;
using System.Diagnostics;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using System.Data;
using Xunit;

namespace DbaClientX.Tests;

public class PostgreSqlTests
{
    [Fact]
    public async Task QueryAsync_InvalidServer_ThrowsDbaQueryExecutionException()
    {
        using var pg = new DBAClientX.PostgreSql();
        var ex = await Assert.ThrowsAsync<DBAClientX.DbaQueryExecutionException>(async () =>
        {
            await pg.QueryAsync("invalid", "postgres", "user", "pass", "SELECT 1").ConfigureAwait(false);
        }).ConfigureAwait(false);
        Assert.Contains("SELECT 1", ex.Message);
    }

    private class PingPostgreSql : DBAClientX.PostgreSql
    {
        public bool ShouldFail { get; set; }

        public override object? ExecuteScalar(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, NpgsqlDbType>? parameterTypes = null)
        {
            if (ShouldFail) throw new DBAClientX.DbaQueryExecutionException("fail", query, new Exception());
            return 1;
        }

        public override Task<object?> ExecuteScalarAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, NpgsqlDbType>? parameterTypes = null)
        {
            if (ShouldFail) throw new DBAClientX.DbaQueryExecutionException("fail", query, new Exception());
            return Task.FromResult<object?>(1);
        }
    }

    [Fact]
    public void Ping_ReturnsTrue_OnSuccess()
    {
        using var pg = new PingPostgreSql { ShouldFail = false };
        Assert.True(pg.Ping("h", "d", "u", "p"));
    }

    [Fact]
    public void Ping_ReturnsFalse_OnFailure()
    {
        using var pg = new PingPostgreSql { ShouldFail = true };
        Assert.False(pg.Ping("h", "d", "u", "p"));
    }

    [Fact]
    public async Task PingAsync_ReturnsTrue_OnSuccess()
    {
        using var pg = new PingPostgreSql { ShouldFail = false };
        Assert.True(await pg.PingAsync("h", "d", "u", "p").ConfigureAwait(false));
    }

    [Fact]
    public async Task PingAsync_ReturnsFalse_OnFailure()
    {
        using var pg = new PingPostgreSql { ShouldFail = true };
        Assert.False(await pg.PingAsync("h", "d", "u", "p").ConfigureAwait(false));
    }

    private class DelayPostgreSql : DBAClientX.PostgreSql
    {
        private readonly TimeSpan _delay;
        private int _current;
        public int MaxConcurrency { get; private set; }

        public DelayPostgreSql(TimeSpan delay)
        {
            _delay = delay;
        }

        public override async Task<object?> QueryAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, NpgsqlDbType>? parameterTypes = null)
        {
            var running = Interlocked.Increment(ref _current);
            try
            {
                MaxConcurrency = Math.Max(MaxConcurrency, running);
                await Task.Delay(_delay, cancellationToken).ConfigureAwait(false);
                return null;
            }
            finally
            {
                Interlocked.Decrement(ref _current);
            }
        }
    }

    [Fact]
    public async Task RunQueriesInParallel_ExecutesConcurrently()
    {
        var queries = Enumerable.Repeat("SELECT 1", 3).ToArray();
        using var pg = new DelayPostgreSql(TimeSpan.FromMilliseconds(200));

        var sequential = Stopwatch.StartNew();
        foreach (var query in queries)
        {
            await pg.QueryAsync("h", "d", "u", "p", query).ConfigureAwait(false);
        }
        sequential.Stop();

        var parallel = Stopwatch.StartNew();
        await pg.RunQueriesInParallel(queries, "h", "d", "u", "p").ConfigureAwait(false);
        parallel.Stop();

        Assert.True(parallel.Elapsed < sequential.Elapsed);
    }

    [Fact]
    public async Task RunQueriesInParallel_RespectsMaxDegreeOfParallelism()
    {
        var queries = Enumerable.Repeat("SELECT 1", 3).ToArray();
        using var pg = new DelayPostgreSql(TimeSpan.FromMilliseconds(200));

        await pg.RunQueriesInParallel(queries, "h", "d", "u", "p", maxDegreeOfParallelism: 1).ConfigureAwait(false);

        Assert.Equal(1, pg.MaxConcurrency);
    }

    [Fact]
    public async Task QueryAsync_CanBeCancelled()
    {
        using var pg = new DelayPostgreSql(TimeSpan.FromSeconds(5));
        using var cts = new CancellationTokenSource(100);
        await Assert.ThrowsAsync<TaskCanceledException>(async () =>
        {
            await pg.QueryAsync("h", "d", "u", "p", "q", cancellationToken: cts.Token).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }

    [Fact]
    public async Task RunQueriesInParallel_ForwardsCancellation()
    {
        using var pg = new DelayPostgreSql(TimeSpan.FromSeconds(5));
        var queries = new[] { "q1", "q2" };
        using var cts = new CancellationTokenSource(100);
        await Assert.ThrowsAsync<TaskCanceledException>(async () =>
        {
            await pg.RunQueriesInParallel(queries, "h", "d", "u", "p", cts.Token).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }

    private class CaptureParametersPostgreSql : DBAClientX.PostgreSql
    {
        public List<(string Name, object? Value, NpgsqlDbType Type)> Captured { get; } = new();

        protected override void AddParameters(DbCommand command, IDictionary<string, object?>? parameters, IDictionary<string, DbType>? parameterTypes = null)
        {
            base.AddParameters(command, parameters, parameterTypes);
            foreach (DbParameter p in command.Parameters)
            {
                if (p is NpgsqlParameter np)
                {
                    Captured.Add((np.ParameterName, np.Value, np.NpgsqlDbType));
                }
            }
        }

        public override Task<object?> QueryAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, NpgsqlDbType>? parameterTypes = null)
        {
            var command = new NpgsqlCommand(query);
            IDictionary<string, DbType>? dbTypes = null;
            if (parameterTypes != null)
            {
                dbTypes = new Dictionary<string, DbType>(parameterTypes.Count);
                foreach (var kv in parameterTypes)
                {
                    var p = new NpgsqlParameter { NpgsqlDbType = kv.Value };
                    dbTypes[kv.Key] = p.DbType;
                }
            }
            AddParameters(command, parameters, dbTypes);
            return Task.FromResult<object?>(null);
        }
    }

    [Fact]
    public async Task QueryAsync_BindsParameters()
    {
        using var pg = new CaptureParametersPostgreSql();
        var parameters = new Dictionary<string, object?>
        {
            ["@id"] = 5,
            ["@name"] = "test"
        };

        await pg.QueryAsync("h", "d", "u", "p", "SELECT 1", parameters).ConfigureAwait(false);

        Assert.Contains(pg.Captured, p => p.Name == "@id" && (int)p.Value == 5);
        Assert.Contains(pg.Captured, p => p.Name == "@name" && (string)p.Value == "test");
    }

    [Fact]
    public async Task QueryAsync_PreservesParameterTypes()
    {
        using var pg = new CaptureParametersPostgreSql();
        var parameters = new Dictionary<string, object?>
        {
            ["@id"] = 5,
            ["@name"] = "test"
        };
        var types = new Dictionary<string, NpgsqlDbType>
        {
            ["@id"] = NpgsqlDbType.Integer,
            ["@name"] = NpgsqlDbType.Text
        };

        await pg.QueryAsync("h", "d", "u", "p", "SELECT 1", parameters, cancellationToken: CancellationToken.None, parameterTypes: types).ConfigureAwait(false);

        Assert.Contains(pg.Captured, p => p.Name == "@id" && p.Type == NpgsqlDbType.Integer);
        Assert.Contains(pg.Captured, p => p.Name == "@name" && p.Type == NpgsqlDbType.Text);
    }

    private class OutputStoredProcPostgreSql : DBAClientX.PostgreSql
    {
        public override object? ExecuteStoredProcedure(string host, string database, string username, string password, string procedure, IEnumerable<DbParameter>? parameters = null, bool useTransaction = false)
        {
            using var command = new NpgsqlCommand();
            AddParameters(command, parameters);
            foreach (NpgsqlParameter p in command.Parameters)
            {
                if (p.Direction != ParameterDirection.Input)
                {
                    p.Value = 5;
                }
            }
            return null;
        }

        public override Task<object?> ExecuteStoredProcedureAsync(string host, string database, string username, string password, string procedure, IEnumerable<DbParameter>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default)
        {
            ExecuteStoredProcedure(host, database, username, password, procedure, parameters, useTransaction);
            return Task.FromResult<object?>(null);
        }
    }

    [Fact]
    public void ExecuteStoredProcedure_PopulatesOutputParameter()
    {
        using var pg = new OutputStoredProcPostgreSql();
        var outParam = new NpgsqlParameter("@out", NpgsqlDbType.Integer) { Direction = ParameterDirection.Output };
        pg.ExecuteStoredProcedure("h", "d", "u", "p", "sp_test", new[] { outParam });
        Assert.Equal(5, outParam.Value);
    }

    [Fact]
    public async Task ExecuteStoredProcedureAsync_PopulatesOutputParameter()
    {
        using var pg = new OutputStoredProcPostgreSql();
        var outParam = new NpgsqlParameter("@out", NpgsqlDbType.Integer) { Direction = ParameterDirection.Output };
        await pg.ExecuteStoredProcedureAsync("h", "d", "u", "p", "sp_test", new[] { outParam }).ConfigureAwait(false);
        Assert.Equal(5, outParam.Value);
    }

    [Fact]
    public void Commit_WithoutTransaction_Throws()
    {
        using var pg = new DBAClientX.PostgreSql();
        Assert.Throws<DBAClientX.DbaTransactionException>(() => pg.Commit());
    }

    [Fact]
    public void Rollback_WithoutTransaction_Throws()
    {
        using var pg = new DBAClientX.PostgreSql();
        Assert.Throws<DBAClientX.DbaTransactionException>(() => pg.Rollback());
    }
}
