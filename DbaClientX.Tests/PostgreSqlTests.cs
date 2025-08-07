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

    private class DelayPostgreSql : DBAClientX.PostgreSql
    {
        private readonly TimeSpan _delay;

        public DelayPostgreSql(TimeSpan delay)
        {
            _delay = delay;
        }

        public override async Task<object?> QueryAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, NpgsqlDbType>? parameterTypes = null)
        {
            await Task.Delay(_delay, cancellationToken).ConfigureAwait(false);
            return null;
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

    private class CaptureStoredProcPostgreSql : DBAClientX.PostgreSql
    {
        public string? CapturedQuery;
        public IDictionary<string, object?>? CapturedParameters;

        public override object? Query(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, NpgsqlDbType>? parameterTypes = null)
        {
            CapturedQuery = query;
            CapturedParameters = parameters;
            return null;
        }

        public override Task<object?> QueryAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, NpgsqlDbType>? parameterTypes = null)
        {
            CapturedQuery = query;
            CapturedParameters = parameters;
            return Task.FromResult<object?>(null);
        }
    }

    [Fact]
    public void ExecuteStoredProcedure_BuildsCallStatement()
    {
        using var pg = new CaptureStoredProcPostgreSql();
        var parameters = new Dictionary<string, object?>
        {
            ["@id"] = 1,
            ["@name"] = "n"
        };
        pg.ExecuteStoredProcedure("h", "d", "u", "p", "sp_test", parameters);
        Assert.Equal("CALL sp_test(@id, @name)", pg.CapturedQuery);
    }

    [Fact]
    public async Task ExecuteStoredProcedureAsync_BuildsCallStatement()
    {
        using var pg = new CaptureStoredProcPostgreSql();
        var parameters = new Dictionary<string, object?>
        {
            ["@id"] = 1
        };
        await pg.ExecuteStoredProcedureAsync("h", "d", "u", "p", "sp_test", parameters).ConfigureAwait(false);
        Assert.Equal("CALL sp_test(@id)", pg.CapturedQuery);
    }

    [Fact]
    public void ExecuteStoredProcedure_NoParameters_AddsEmptyParentheses()
    {
        using var pg = new CaptureStoredProcPostgreSql();
        pg.ExecuteStoredProcedure("h", "d", "u", "p", "sp_test", null);
        Assert.Equal("CALL sp_test()", pg.CapturedQuery);
    }
}
