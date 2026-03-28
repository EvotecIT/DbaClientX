using System;
using DBAClientX;
using Npgsql;
using NpgsqlTypes;
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using System.Data;
using System.Reflection;
using System.Runtime.CompilerServices;
using Xunit;

namespace DbaClientX.Tests;

public class PostgreSqlTests
{
    private static readonly FieldInfo TransactionField = typeof(DBAClientX.PostgreSql).GetField("_transaction", BindingFlags.Instance | BindingFlags.NonPublic)!;
    private static readonly FieldInfo TransactionConnectionField = typeof(DBAClientX.PostgreSql).GetField("_transactionConnection", BindingFlags.Instance | BindingFlags.NonPublic)!;
    private static readonly FieldInfo TransactionConnectionStringField = typeof(DBAClientX.PostgreSql).GetField("_transactionConnectionString", BindingFlags.Instance | BindingFlags.NonPublic)!;

    [Fact]
    public async Task QueryAsync_InvalidServer_ThrowsDbaQueryExecutionException()
    {
        using var pg = new DBAClientX.PostgreSql();
        var ex = await Assert.ThrowsAsync<DBAClientX.DbaQueryExecutionException>(async () =>
        {
            await pg.QueryAsync("invalid", "postgres", "user", "pass", "SELECT 1");
        });
        Assert.Contains("SELECT 1", ex.Message);
    }

    private class PingPostgreSql : DBAClientX.PostgreSql
    {
        public bool ShouldFail { get; set; }

        public override object? ExecuteScalar(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, NpgsqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            if (ShouldFail) throw new DBAClientX.DbaQueryExecutionException("fail", query, new Exception());
            return 1;
        }

        public override Task<object?> ExecuteScalarAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, NpgsqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
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
        Assert.True(await pg.PingAsync("h", "d", "u", "p"));
    }

    [Fact]
    public async Task PingAsync_ReturnsFalse_OnFailure()
    {
        using var pg = new PingPostgreSql { ShouldFail = true };
        Assert.False(await pg.PingAsync("h", "d", "u", "p"));
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

        public override async Task<object?> QueryAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, NpgsqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
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
    public async Task RunQueriesInParallel_ExecutesConcurrently()
    {
        var queries = Enumerable.Repeat("SELECT 1", 3).ToArray();
        using var pg = new DelayPostgreSql(TimeSpan.FromMilliseconds(200));

        await pg.RunQueriesInParallel(queries, "h", "d", "u", "p");

        Assert.True(pg.MaxConcurrency > 1);
    }

    [Fact]
    public async Task RunQueriesInParallel_RespectsMaxDegreeOfParallelism()
    {
        var queries = Enumerable.Repeat("SELECT 1", 3).ToArray();
        using var pg = new DelayPostgreSql(TimeSpan.FromMilliseconds(200));

        await pg.RunQueriesInParallel(queries, "h", "d", "u", "p", maxDegreeOfParallelism: 1);

        Assert.Equal(1, pg.MaxConcurrency);
    }

    [Fact]
    public async Task RunQueriesInParallel_UsesDefaultThrottling()
    {
        var queries = Enumerable.Repeat("SELECT 1", DBAClientX.PostgreSql.DefaultMaxParallelQueries * 4).ToArray();
        using var pg = new DelayPostgreSql(TimeSpan.FromMilliseconds(100));

        await pg.RunQueriesInParallel(queries, "h", "d", "u", "p");

        Assert.InRange(pg.MaxConcurrency, 1, DBAClientX.PostgreSql.DefaultMaxParallelQueries);
    }

    [Fact]
    public async Task QueryAsync_CanBeCancelled()
    {
        using var pg = new DelayPostgreSql(TimeSpan.FromSeconds(5));
        using var cts = new CancellationTokenSource(100);
        await Assert.ThrowsAsync<TaskCanceledException>(async () =>
        {
            await pg.QueryAsync("h", "d", "u", "p", "q", cancellationToken: cts.Token);
        });
    }

    [Fact]
    public async Task RunQueriesInParallel_ForwardsCancellation()
    {
        using var pg = new DelayPostgreSql(TimeSpan.FromSeconds(5));
        var queries = new[] { "q1", "q2" };
        using var cts = new CancellationTokenSource(100);
        await Assert.ThrowsAsync<TaskCanceledException>(async () =>
        {
            await pg.RunQueriesInParallel(queries, "h", "d", "u", "p", cts.Token);
        });
    }

    [Fact]
    public async Task RunQueriesInParallel_WithBlankQuery_ThrowsBeforeStartingWork()
    {
        using var pg = new DelayPostgreSql(TimeSpan.FromSeconds(5));

        var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
            pg.RunQueriesInParallel(new[] { "SELECT 1", " " }, "h", "d", "u", "p"));

        Assert.Equal("queries", exception.ParamName);
        Assert.Equal(0, pg.MaxConcurrency);
    }

    private class OpenFailurePostgreSql : DBAClientX.PostgreSql
    {
        public int SyncDisposeCalls { get; private set; }
        public int AsyncDisposeCalls { get; private set; }

        protected override void OpenConnection(NpgsqlConnection connection)
            => throw new InvalidOperationException("boom");

        protected override Task OpenConnectionAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
            => Task.FromException(new InvalidOperationException("boom"));

        protected override void DisposeConnection(NpgsqlConnection connection)
            => SyncDisposeCalls++;

        protected override ValueTask DisposeConnectionAsync(NpgsqlConnection connection)
        {
            AsyncDisposeCalls++;
            return default;
        }
    }

    [Fact]
    public void ExecuteNonQuery_WhenOpenFails_DisposesConnection()
    {
        using var pg = new OpenFailurePostgreSql();

        var ex = Assert.Throws<DBAClientX.DbaQueryExecutionException>(() => pg.ExecuteNonQuery("h", "d", "u", "p", "UPDATE t SET c = 1"));

        Assert.IsType<InvalidOperationException>(ex.InnerException);
        Assert.Equal(1, pg.SyncDisposeCalls);
        Assert.Equal(0, pg.AsyncDisposeCalls);
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_WhenOpenFails_DisposesConnection()
    {
        using var pg = new OpenFailurePostgreSql();

        var ex = await Assert.ThrowsAsync<DBAClientX.DbaQueryExecutionException>(() => pg.ExecuteNonQueryAsync("h", "d", "u", "p", "UPDATE t SET c = 1"));

        Assert.IsType<InvalidOperationException>(ex.InnerException);
        Assert.Equal(0, pg.SyncDisposeCalls);
        Assert.Equal(1, pg.AsyncDisposeCalls);
    }

    [Fact]
    public async Task QueryAsync_WhenOpenFails_DisposesConnectionAsynchronously()
    {
        using var pg = new OpenFailurePostgreSql();

        var ex = await Assert.ThrowsAsync<DBAClientX.DbaQueryExecutionException>(() => pg.QueryAsync("h", "d", "u", "p", "SELECT 1"));

        Assert.IsType<InvalidOperationException>(ex.InnerException);
        Assert.Equal(0, pg.SyncDisposeCalls);
        Assert.Equal(1, pg.AsyncDisposeCalls);
    }

    [Fact]
    public async Task ExecuteScalarAsync_WhenOpenFails_DisposesConnectionAsynchronously()
    {
        using var pg = new OpenFailurePostgreSql();

        var ex = await Assert.ThrowsAsync<DBAClientX.DbaQueryExecutionException>(() => pg.ExecuteScalarAsync("h", "d", "u", "p", "SELECT 1"));

        Assert.IsType<InvalidOperationException>(ex.InnerException);
        Assert.Equal(0, pg.SyncDisposeCalls);
        Assert.Equal(1, pg.AsyncDisposeCalls);
    }

    [Fact]
    public async Task ExecuteStoredProcedureAsync_WhenOpenFails_DisposesConnectionAsynchronously()
    {
        using var pg = new OpenFailurePostgreSql();

        var ex = await Assert.ThrowsAsync<DBAClientX.DbaQueryExecutionException>(() => pg.ExecuteStoredProcedureAsync("h", "d", "u", "p", "sp_test", parameters: (IDictionary<string, object?>?)null));

        Assert.IsType<InvalidOperationException>(ex.InnerException);
        Assert.Equal(0, pg.SyncDisposeCalls);
        Assert.Equal(1, pg.AsyncDisposeCalls);
    }

    private class SeededTransactionPostgreSql : DBAClientX.PostgreSql
    {
        public void SeedActiveTransaction(string connectionString)
        {
            TransactionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(NpgsqlTransaction)));
            TransactionConnectionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(NpgsqlConnection)));
            TransactionConnectionStringField.SetValue(this, connectionString);
        }
    }

    [Fact]
    public void ExecuteNonQuery_WithMismatchedTransactionConnection_Throws()
    {
        using var pg = new SeededTransactionPostgreSql();
        pg.SeedActiveTransaction(DBAClientX.PostgreSql.BuildConnectionString("h1", "d1", "u1", "p1"));

        var ex = Assert.Throws<DBAClientX.DbaQueryExecutionException>(() => pg.ExecuteNonQuery("h2", "d2", "u2", "p2", "UPDATE t SET c = 1", useTransaction: true));

        Assert.IsType<DBAClientX.DbaTransactionException>(ex.InnerException);
    }

    private class CaptureParametersPostgreSql : DBAClientX.PostgreSql
    {
        public List<(string Name, object? Value, NpgsqlDbType Type)> Captured { get; } = new();

        protected override void AddParameters(DbCommand command, IDictionary<string, object?>? parameters, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            base.AddParameters(command, parameters, parameterTypes, parameterDirections);
            foreach (DbParameter p in command.Parameters)
            {
                if (p is NpgsqlParameter np)
                {
                    Captured.Add((np.ParameterName, np.Value, np.NpgsqlDbType));
                }
            }
        }

        public override Task<object?> QueryAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, NpgsqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            var command = new NpgsqlCommand(query);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            AddParameters(command, parameters, dbTypes, parameterDirections);
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

        await pg.QueryAsync("h", "d", "u", "p", "SELECT 1", parameters);

        Assert.Contains(pg.Captured, p => p.Name == "@id" && p.Value is int v && v == 5);
        Assert.Contains(pg.Captured, p => p.Name == "@name" && p.Value is string s && s == "test");
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

        await pg.QueryAsync("h", "d", "u", "p", "SELECT 1", parameters, cancellationToken: CancellationToken.None, parameterTypes: types);

        Assert.Contains(pg.Captured, p => p.Name == "@id" && p.Type == NpgsqlDbType.Integer);
        Assert.Contains(pg.Captured, p => p.Name == "@name" && p.Type == NpgsqlDbType.Text);
    }

    [Fact]
    public async Task QueryAsync_PreservesProviderSpecificParameterTypes()
    {
        using var pg = new CaptureParametersPostgreSql();
        var parameters = new Dictionary<string, object?>
        {
            ["@json"] = "{\"a\":1}",
            ["@ip"] = "127.0.0.1"
        };
        var types = new Dictionary<string, NpgsqlDbType>
        {
            ["@json"] = NpgsqlDbType.Jsonb,
            ["@ip"] = NpgsqlDbType.Inet
        };

        await pg.QueryAsync("h", "d", "u", "p", "SELECT 1", parameters, cancellationToken: CancellationToken.None, parameterTypes: types);

        Assert.Contains(pg.Captured, p => p.Name == "@json" && p.Type == NpgsqlDbType.Jsonb);
        Assert.Contains(pg.Captured, p => p.Name == "@ip" && p.Type == NpgsqlDbType.Inet);
    }

    private class OutputDictionaryPostgreSql : DBAClientX.PostgreSql
    {
        public override object? Query(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, NpgsqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            using var command = new NpgsqlCommand();
            var dbTypes = ConvertParameterTypes(parameterTypes);
            AddParameters(command, parameters, dbTypes, parameterDirections);
            foreach (NpgsqlParameter p in command.Parameters)
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
    public void Query_UpdatesOutputParameters()
    {
        using var pg = new OutputDictionaryPostgreSql();
        var parameters = new Dictionary<string, object?> { ["@out"] = null };
        var directions = new Dictionary<string, ParameterDirection> { ["@out"] = ParameterDirection.Output };
        pg.Query("h", "d", "u", "p", "q", parameters, parameterDirections: directions);
        Assert.Equal(5, parameters["@out"]);
    }

    private class CaptureStoredProcPostgreSql : DBAClientX.PostgreSql
    {
        public List<NpgsqlParameter> Captured { get; } = new();
        public CommandType CapturedCommandType { get; private set; }

        public override object? ExecuteStoredProcedure(string host, string database, string username, string password, string procedure, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, NpgsqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            using var command = new NpgsqlCommand(procedure);
            command.CommandType = CommandType.StoredProcedure;
            var dbTypes = ConvertParameterTypes(parameterTypes);
            AddParameters(command, parameters, dbTypes, parameterDirections);
            CapturedCommandType = command.CommandType;
            foreach (NpgsqlParameter p in command.Parameters)
            {
                Captured.Add(p);
            }
            return null;
        }

        public override Task<object?> ExecuteStoredProcedureAsync(string host, string database, string username, string password, string procedure, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, NpgsqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            ExecuteStoredProcedure(host, database, username, password, procedure, parameters, useTransaction, parameterTypes, parameterDirections);
            return Task.FromResult<object?>(null);
        }
    }

    [Fact]
    public void ExecuteStoredProcedureDictionary_BindsParameters()
    {
        using var pg = new CaptureStoredProcPostgreSql();
        var parameters = new Dictionary<string, object?>
        {
            ["@id"] = 1,
            ["@name"] = "n"
        };
        pg.ExecuteStoredProcedure("h", "d", "u", "p", "sp_test", parameters);
        Assert.Equal(CommandType.StoredProcedure, pg.CapturedCommandType);
        Assert.Contains(pg.Captured, p => p.ParameterName == "@id" && p.Value is int v && v == 1);
        Assert.Contains(pg.Captured, p => p.ParameterName == "@name" && p.Value is string s && s == "n");
    }

    [Fact]
    public void ExecuteStoredProcedureDictionary_NoParameters_AddsNoParameters()
    {
        using var pg = new CaptureStoredProcPostgreSql();
        pg.ExecuteStoredProcedure("h", "d", "u", "p", "sp_test", (IDictionary<string, object?>?)null);
        Assert.Equal(CommandType.StoredProcedure, pg.CapturedCommandType);
        Assert.Empty(pg.Captured);
    }

    [Fact]
    public async Task ExecuteStoredProcedureDictionaryAsync_PreservesParameterTypes()
    {
        using var pg = new CaptureStoredProcPostgreSql();
        var parameters = new Dictionary<string, object?>
        {
            ["@id"] = 5
        };
        var types = new Dictionary<string, NpgsqlDbType>
        {
            ["@id"] = NpgsqlDbType.Integer
        };

        await pg.ExecuteStoredProcedureAsync("h", "d", "u", "p", "sp_test", parameters, parameterTypes: types);

        Assert.Contains(pg.Captured, p => p.ParameterName == "@id" && p.NpgsqlDbType == NpgsqlDbType.Integer);
    }

    [Fact]
    public async Task ExecuteStoredProcedureDictionaryAsync_PreservesProviderSpecificParameterTypes()
    {
        using var pg = new CaptureStoredProcPostgreSql();
        var parameters = new Dictionary<string, object?>
        {
            ["@json"] = "{\"a\":1}"
        };
        var types = new Dictionary<string, NpgsqlDbType>
        {
            ["@json"] = NpgsqlDbType.Jsonb
        };

        await pg.ExecuteStoredProcedureAsync("h", "d", "u", "p", "sp_test", parameters, parameterTypes: types);

        Assert.Contains(pg.Captured, p => p.ParameterName == "@json" && p.NpgsqlDbType == NpgsqlDbType.Jsonb);
    }

    private class OutputDictionaryStoredProcPostgreSql : DBAClientX.PostgreSql
    {
        public override object? ExecuteStoredProcedure(string host, string database, string username, string password, string procedure, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, NpgsqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            using var command = new NpgsqlCommand();
            var dbTypes = ConvertParameterTypes(parameterTypes);
            AddParameters(command, parameters, dbTypes, parameterDirections);
            foreach (NpgsqlParameter p in command.Parameters)
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
    public void ExecuteStoredProcedureDictionary_UpdatesOutputParameters()
    {
        using var pg = new OutputDictionaryStoredProcPostgreSql();
        var parameters = new Dictionary<string, object?> { ["@out"] = null };
        var directions = new Dictionary<string, ParameterDirection> { ["@out"] = ParameterDirection.Output };
        pg.ExecuteStoredProcedure("h", "d", "u", "p", "sp_test", parameters, parameterDirections: directions);
        Assert.Equal(5, parameters["@out"]);
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
        await pg.ExecuteStoredProcedureAsync("h", "d", "u", "p", "sp_test", new[] { outParam });
        Assert.Equal(5, outParam.Value);
    }

    [Fact]
    public void Query_WithEmptySql_Throws()
    {
        using var pg = new DBAClientX.PostgreSql();

        Assert.Throws<ArgumentException>(() => pg.Query("h", "d", "u", "p", " "));
    }

    [Fact]
    public void ExecuteStoredProcedure_WithEmptyProcedure_Throws()
    {
        using var pg = new DBAClientX.PostgreSql();

        Assert.Throws<ArgumentException>(() => pg.ExecuteStoredProcedure("h", "d", "u", "p", " ", parameters: (IDictionary<string, object?>?)null));
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
