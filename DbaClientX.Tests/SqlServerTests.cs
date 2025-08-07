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
            await sqlServer.QueryAsync("invalid", "master", true, "SELECT 1").ConfigureAwait(false);
        }).ConfigureAwait(false);
        Assert.Contains("SELECT 1", ex.Message);
    }

    private class DelaySqlServer : DBAClientX.SqlServer
    {
        private readonly TimeSpan _delay;

        public DelaySqlServer(TimeSpan delay)
        {
            _delay = delay;
        }

        public override async Task<object?> QueryAsync(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, SqlDbType>? parameterTypes = null, string? username = null, string? password = null)
        {
            await Task.Delay(_delay, cancellationToken).ConfigureAwait(false);
            return null;
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
            await sqlServer.QueryAsync("ignored", "ignored", true, query).ConfigureAwait(false);
        }
        sequential.Stop();

        var parallel = Stopwatch.StartNew();
        await sqlServer.RunQueriesInParallel(queries, "ignored", "ignored", true).ConfigureAwait(false);
        parallel.Stop();

        Assert.True(parallel.Elapsed < sequential.Elapsed);
    }

    [Fact]
    public async Task QueryAsync_CanBeCancelled()
    {
        using var sqlServer = new DelaySqlServer(TimeSpan.FromSeconds(5));
        using var cts = new CancellationTokenSource(100);
        await Assert.ThrowsAsync<TaskCanceledException>(async () =>
        {
            await sqlServer.QueryAsync("s", "db", true, "q", cancellationToken: cts.Token).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }

    [Fact]
    public async Task RunQueriesInParallel_ForwardsCancellation()
    {
        using var sqlServer = new DelaySqlServer(TimeSpan.FromSeconds(5));
        var queries = new[] { "q1", "q2" };
        using var cts = new CancellationTokenSource(100);
        await Assert.ThrowsAsync<TaskCanceledException>(async () =>
        {
            await sqlServer.RunQueriesInParallel(queries, "s", "db", true, cts.Token).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }

    private class CaptureParametersSqlServer : DBAClientX.SqlServer
    {
        public List<(string Name, object? Value, SqlDbType Type)> Captured { get; } = new();

        protected override void AddParameters(DbCommand command, IDictionary<string, object?>? parameters, IDictionary<string, DbType>? parameterTypes = null)
        {
            base.AddParameters(command, parameters, parameterTypes);
            foreach (DbParameter p in command.Parameters)
            {
                if (p is SqlParameter sp)
                {
                    Captured.Add((sp.ParameterName, sp.Value, sp.SqlDbType));
                }
            }
        }

        public override Task<object?> QueryAsync(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, SqlDbType>? parameterTypes = null, string? username = null, string? password = null)
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
            AddParameters(command, parameters, dbTypes);
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

        await sqlServer.QueryAsync("ignored", "ignored", true, "SELECT 1", parameters).ConfigureAwait(false);

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

        await sqlServer.QueryAsync("ignored", "ignored", true, "SELECT 1", parameters, cancellationToken: CancellationToken.None, parameterTypes: types).ConfigureAwait(false);

        Assert.Contains(sqlServer.Captured, p => p.Name == "@id" && p.Type == SqlDbType.Int);
        Assert.Contains(sqlServer.Captured, p => p.Name == "@name" && p.Type == SqlDbType.NVarChar);
    }

    private class CaptureStoredProcSqlServer : DBAClientX.SqlServer
    {
        public string? CapturedQuery;
        public IDictionary<string, object?>? CapturedParameters;

        public override object? Query(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, SqlDbType>? parameterTypes = null, string? username = null, string? password = null)
        {
            CapturedQuery = query;
            CapturedParameters = parameters;
            return null;
        }

        public override Task<object?> QueryAsync(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, SqlDbType>? parameterTypes = null, string? username = null, string? password = null)
        {
            CapturedQuery = query;
            CapturedParameters = parameters;
            return Task.FromResult<object?>(null);
        }
    }

    [Fact]
    public void ExecuteStoredProcedure_BuildsExecStatement()
    {
        using var sqlServer = new CaptureStoredProcSqlServer();
        var parameters = new Dictionary<string, object?>
        {
            ["@id"] = 1,
            ["@name"] = "n"
        };
        sqlServer.ExecuteStoredProcedure("s", "db", true, "sp_test", parameters);
        Assert.Equal("EXEC sp_test @id, @name", sqlServer.CapturedQuery);
    }

    [Fact]
    public async Task ExecuteStoredProcedureAsync_BuildsExecStatement()
    {
        using var sqlServer = new CaptureStoredProcSqlServer();
        var parameters = new Dictionary<string, object?>
        {
            ["@id"] = 1
        };
        await sqlServer.ExecuteStoredProcedureAsync("s", "db", true, "sp_test", parameters).ConfigureAwait(false);
        Assert.Equal("EXEC sp_test @id", sqlServer.CapturedQuery);
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

        public override object? Query(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, SqlDbType>? parameterTypes = null, string? username = null, string? password = null)
        {
            if (useTransaction && !TransactionStarted) throw new DBAClientX.DbaTransactionException("Transaction has not been started.");
            return null;
        }

        public override Task<object?> QueryAsync(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, SqlDbType>? parameterTypes = null, string? username = null, string? password = null)
        {
            return Task.FromResult<object?>(Query(serverOrInstance, database, integratedSecurity, query, parameters, useTransaction, parameterTypes, username, password));
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
