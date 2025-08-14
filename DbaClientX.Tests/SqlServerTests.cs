using DBAClientX;
using System.Data.SqlClient;
using System.Data.Common;
using System.Diagnostics;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using System.Data;
using System.Reflection;
using Xunit;

namespace DbaClientX.Tests;

public class SqlServerTests
{
    [Fact]
    public async Task QueryAsync_InvalidServer_ThrowsDbaQueryExecutionException()
    {
        using var sqlServer = new DBAClientX.SqlServer();
        var ex = await Assert.ThrowsAsync<DBAClientX.DbaQueryExecutionException>(async () =>
        {
            await sqlServer.QueryAsync("invalid", "master", true, "SELECT 1");
        });
        Assert.Contains("SELECT 1", ex.Message);
    }

    private class PingSqlServer : DBAClientX.SqlServer
    {
        public bool ShouldFail { get; set; }

        public override object? ExecuteScalar(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, SqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null, string? username = null, string? password = null)
        {
            if (ShouldFail) throw new DBAClientX.DbaQueryExecutionException("fail", query, new Exception());
            return 1;
        }

        public override Task<object?> ExecuteScalarAsync(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, SqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null, string? username = null, string? password = null)
        {
            if (ShouldFail) throw new DBAClientX.DbaQueryExecutionException("fail", query, new Exception());
            return Task.FromResult<object?>(1);
        }
    }

    [Fact]
    public void Ping_ReturnsTrue_OnSuccess()
    {
        using var sqlServer = new PingSqlServer { ShouldFail = false };
        Assert.True(sqlServer.Ping("s", "db", true));
    }

    [Fact]
    public void Ping_ReturnsFalse_OnFailure()
    {
        using var sqlServer = new PingSqlServer { ShouldFail = true };
        Assert.False(sqlServer.Ping("s", "db", true));
    }

    [Fact]
    public async Task PingAsync_ReturnsTrue_OnSuccess()
    {
        using var sqlServer = new PingSqlServer { ShouldFail = false };
        Assert.True(await sqlServer.PingAsync("s", "db", true));
    }

    [Fact]
    public async Task PingAsync_ReturnsFalse_OnFailure()
    {
        using var sqlServer = new PingSqlServer { ShouldFail = true };
        Assert.False(await sqlServer.PingAsync("s", "db", true));
    }

    private class DelaySqlServer : DBAClientX.SqlServer
    {
        private readonly TimeSpan _delay;
        private int _current;
        public int MaxConcurrency { get; private set; }

        public DelaySqlServer(TimeSpan delay)
        {
            _delay = delay;
        }

        public override async Task<object?> QueryAsync(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, SqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null, string? username = null, string? password = null)
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
        using var sqlServer = new DelaySqlServer(TimeSpan.FromMilliseconds(200));

        var sequential = Stopwatch.StartNew();
        foreach (var query in queries)
        {
            await sqlServer.QueryAsync("ignored", "ignored", true, query);
        }
        sequential.Stop();

        var parallel = Stopwatch.StartNew();
        await sqlServer.RunQueriesInParallel(queries, "ignored", "ignored", true);
        parallel.Stop();

        Assert.True(parallel.Elapsed < sequential.Elapsed);
    }

    [Fact]
    public async Task RunQueriesInParallel_RespectsMaxDegreeOfParallelism()
    {
        var queries = Enumerable.Repeat("SELECT 1", 3).ToArray();
        using var sqlServer = new DelaySqlServer(TimeSpan.FromMilliseconds(200));

        await sqlServer.RunQueriesInParallel(queries, "s", "db", true, maxDegreeOfParallelism: 1);

        Assert.Equal(1, sqlServer.MaxConcurrency);
    }

    [Fact]
    public async Task QueryAsync_CanBeCancelled()
    {
        using var sqlServer = new DelaySqlServer(TimeSpan.FromSeconds(5));
        using var cts = new CancellationTokenSource(100);
        await Assert.ThrowsAsync<TaskCanceledException>(async () =>
        {
            await sqlServer.QueryAsync("s", "db", true, "q", cancellationToken: cts.Token);
        });
    }

    [Fact]
    public async Task RunQueriesInParallel_ForwardsCancellation()
    {
        using var sqlServer = new DelaySqlServer(TimeSpan.FromSeconds(5));
        var queries = new[] { "q1", "q2" };
        using var cts = new CancellationTokenSource(100);
        await Assert.ThrowsAsync<TaskCanceledException>(async () =>
        {
            await sqlServer.RunQueriesInParallel(queries, "s", "db", true, cts.Token);
        });
    }

    private class CaptureParametersSqlServer : DBAClientX.SqlServer
    {
        public List<(string Name, object? Value, SqlDbType Type)> Captured { get; } = new();

        protected override void AddParameters(DbCommand command, IDictionary<string, object?>? parameters, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            base.AddParameters(command, parameters, parameterTypes, parameterDirections);
            foreach (DbParameter p in command.Parameters)
            {
                if (p is SqlParameter sp)
                {
                    Captured.Add((sp.ParameterName, sp.Value, sp.SqlDbType));
                }
            }
        }

        public override Task<object?> QueryAsync(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, SqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null, string? username = null, string? password = null)
        {
            var command = new SqlCommand(query);
            IDictionary<string, DbType>? dbTypes = null;
            if (parameterTypes != null)
            {
                dbTypes = new Dictionary<string, DbType>(parameterTypes.Count);
                foreach (var kv in parameterTypes)
                {
                    var p = new SqlParameter { SqlDbType = kv.Value };
                    dbTypes[kv.Key] = p.DbType;
                }
            }
            AddParameters(command, parameters, dbTypes, parameterDirections);
            return Task.FromResult<object?>(null);
        }
    }

    [Fact]
    public async Task QueryAsync_BindsParameters()
    {
        using var sqlServer = new CaptureParametersSqlServer();
        var parameters = new Dictionary<string, object?>
        {
            ["@id"] = 5,
            ["@name"] = "test"
        };

        await sqlServer.QueryAsync("ignored", "ignored", true, "SELECT 1", parameters);

        Assert.Contains(sqlServer.Captured, p => p.Name == "@id" && (int)p.Value == 5);
        Assert.Contains(sqlServer.Captured, p => p.Name == "@name" && (string)p.Value == "test");
    }

    [Fact]
    public async Task QueryAsync_PreservesParameterTypes()
    {
        using var sqlServer = new CaptureParametersSqlServer();
        var parameters = new Dictionary<string, object?>
        {
            ["@id"] = 5,
            ["@name"] = "test"
        };
        var types = new Dictionary<string, SqlDbType>
        {
            ["@id"] = SqlDbType.Int,
            ["@name"] = SqlDbType.NVarChar
        };

        await sqlServer.QueryAsync("ignored", "ignored", true, "SELECT 1", parameters, cancellationToken: CancellationToken.None, parameterTypes: types);

        Assert.Contains(sqlServer.Captured, p => p.Name == "@id" && p.Type == SqlDbType.Int);
        Assert.Contains(sqlServer.Captured, p => p.Name == "@name" && p.Type == SqlDbType.NVarChar);
    }

    private class OutputDictionarySqlServer : DBAClientX.SqlServer
    {
        public override object? Query(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, SqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null, string? username = null, string? password = null)
        {
            using var command = new SqlCommand();
            var dbTypes = DbTypeConverter.ConvertParameterTypes(parameterTypes, static () => new SqlParameter(), static (p, t) => p.SqlDbType = t);
            AddParameters(command, parameters, dbTypes, parameterDirections);
            foreach (SqlParameter p in command.Parameters)
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
        using var sqlServer = new OutputDictionarySqlServer();
        var parameters = new Dictionary<string, object?> { ["@out"] = null };
        var directions = new Dictionary<string, ParameterDirection> { ["@out"] = ParameterDirection.Output };
        sqlServer.Query("s", "d", true, "q", parameters, parameterDirections: directions);
        Assert.Equal(5, parameters["@out"]);
    }

    private class OutputStoredProcSqlServer : DBAClientX.SqlServer
    {
        public override object? ExecuteStoredProcedure(string serverOrInstance, string database, bool integratedSecurity, string procedure, IEnumerable<DbParameter>? parameters = null, bool useTransaction = false, string? username = null, string? password = null)
        {
            using var command = new SqlCommand();
            AddParameters(command, parameters);
            foreach (SqlParameter p in command.Parameters)
            {
                if (p.Direction != ParameterDirection.Input)
                {
                    p.Value = 5;
                }
            }
            return null;
        }

        public override Task<object?> ExecuteStoredProcedureAsync(string serverOrInstance, string database, bool integratedSecurity, string procedure, IEnumerable<DbParameter>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, string? username = null, string? password = null)
        {
            ExecuteStoredProcedure(serverOrInstance, database, integratedSecurity, procedure, parameters, useTransaction, username, password);
            return Task.FromResult<object?>(null);
        }
    }

    [Fact]
    public void ExecuteStoredProcedure_PopulatesOutputParameter()
    {
        using var sqlServer = new OutputStoredProcSqlServer();
        var outParam = new SqlParameter("@out", SqlDbType.Int) { Direction = ParameterDirection.Output };
        sqlServer.ExecuteStoredProcedure("s", "db", true, "sp_test", new[] { outParam });
        Assert.Equal(5, outParam.Value);
    }

    [Fact]
    public async Task ExecuteStoredProcedureAsync_PopulatesOutputParameter()
    {
        using var sqlServer = new OutputStoredProcSqlServer();
        var outParam = new SqlParameter("@out", SqlDbType.Int) { Direction = ParameterDirection.Output };
        await sqlServer.ExecuteStoredProcedureAsync("s", "db", true, "sp_test", new[] { outParam });
        Assert.Equal(5, outParam.Value);
    }

    private class FakeTransactionSqlServer : DBAClientX.SqlServer
    {
        public bool TransactionStarted { get; private set; }

        public override void BeginTransaction(string serverOrInstance, string database, bool integratedSecurity, string? username = null, string? password = null)
        {
            TransactionStarted = true;
        }

        public override void Commit()
        {
            if (!TransactionStarted) throw new DBAClientX.DbaTransactionException("No active transaction.");
            TransactionStarted = false;
        }

        public override void Rollback()
        {
            if (!TransactionStarted) throw new DBAClientX.DbaTransactionException("No active transaction.");
            TransactionStarted = false;
        }

        public override object? Query(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, SqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null, string? username = null, string? password = null)
        {
            if (useTransaction && !TransactionStarted) throw new DBAClientX.DbaTransactionException("Transaction has not been started.");
            return null;
        }

        public override Task<object?> QueryAsync(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, SqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null, string? username = null, string? password = null)
        {
            return Task.FromResult<object?>(Query(serverOrInstance, database, integratedSecurity, query, parameters, useTransaction, parameterTypes, parameterDirections, username, password));
        }
    }

    [Fact]
    public void Query_WithTransactionNotStarted_Throws()
    {
        using var sqlServer = new FakeTransactionSqlServer();
        Assert.Throws<DBAClientX.DbaTransactionException>(() => sqlServer.Query("s", "db", true, "q", null, true));
    }

    [Fact]
    public void Commit_WithoutTransaction_Throws()
    {
        using var sqlServer = new DBAClientX.SqlServer();
        Assert.Throws<DBAClientX.DbaTransactionException>(() => sqlServer.Commit());
    }

    [Fact]
    public void Rollback_WithoutTransaction_Throws()
    {
        using var sqlServer = new DBAClientX.SqlServer();
        Assert.Throws<DBAClientX.DbaTransactionException>(() => sqlServer.Rollback());
    }

    [Fact]
    public void Commit_EndsTransaction()
    {
        using var sqlServer = new FakeTransactionSqlServer();
        sqlServer.BeginTransaction("s", "db", true);
        sqlServer.Commit();
        Assert.Throws<DBAClientX.DbaTransactionException>(() => sqlServer.Query("s", "db", true, "q", null, true));
    }

    [Fact]
    public void Rollback_EndsTransaction()
    {
        using var sqlServer = new FakeTransactionSqlServer();
        sqlServer.BeginTransaction("s", "db", true);
        sqlServer.Rollback();
        Assert.Throws<DBAClientX.DbaTransactionException>(() => sqlServer.Query("s", "db", true, "q", null, true));
    }

    [Fact]
    public void Query_UsesTransaction_WhenStarted()
    {
        using var sqlServer = new FakeTransactionSqlServer();
        sqlServer.BeginTransaction("s", "db", true);
        var ex = Record.Exception(() => sqlServer.Query("s", "db", true, "q", null, true));
        Assert.Null(ex);
    }

    private class TestClient : DBAClientX.DatabaseClientBase
    {
    }

    [Fact]
    public void BuildResult_ReturnsDataTable_ForDataTableReturnType()
    {
        using var client = new TestClient { ReturnType = DBAClientX.ReturnType.DataTable };
        var ds = new DataSet();
        var table = new DataTable();
        table.Columns.Add("id", typeof(int));
        table.Rows.Add(1);
        ds.Tables.Add(table);

        var method = typeof(DBAClientX.DatabaseClientBase)
            .GetMethod("BuildResult", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var result = method.Invoke(client, new object[] { ds });

        var dt = Assert.IsType<DataTable>(result);
        Assert.Equal(1, dt.Rows.Count);
    }

    [Fact]
    public void BuildResult_ReturnsDataRow_ForDataRowReturnType()
    {
        using var client = new TestClient { ReturnType = DBAClientX.ReturnType.DataRow };
        var ds = new DataSet();
        var table = new DataTable();
        table.Columns.Add("id", typeof(int));
        table.Rows.Add(2);
        ds.Tables.Add(table);

        var method = typeof(DBAClientX.DatabaseClientBase)
            .GetMethod("BuildResult", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var result = method.Invoke(client, new object[] { ds });

        var row = Assert.IsType<DataRow>(result);
        Assert.Equal(2, row["id"]);
    }
}
