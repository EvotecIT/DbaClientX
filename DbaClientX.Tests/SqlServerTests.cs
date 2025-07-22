using System.Data.SqlClient;
using System.Diagnostics;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace DbaClientX.Tests;

public class SqlServerTests
{
    [Fact]
    public async Task SqlQueryAsync_InvalidServer_ThrowsDbaQueryExecutionException()
    {
        var sqlServer = new DBAClientX.SqlServer();
        await Assert.ThrowsAsync<DBAClientX.DbaQueryExecutionException>(async () =>
        {
            await sqlServer.SqlQueryAsync("invalid", "master", true, "SELECT 1");
        });
    }

    private class DelaySqlServer : DBAClientX.SqlServer
    {
        private readonly TimeSpan _delay;

        public DelaySqlServer(TimeSpan delay)
        {
            _delay = delay;
        }

        public override async Task<object?> SqlQueryAsync(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false)
        {
            await Task.Delay(_delay);
            return null;
        }
    }

    [Fact]
    public async Task RunQueriesInParallel_ExecutesConcurrently()
    {
        var queries = Enumerable.Repeat("SELECT 1", 3).ToArray();
        var sqlServer = new DelaySqlServer(TimeSpan.FromMilliseconds(200));

        var sequential = Stopwatch.StartNew();
        foreach (var query in queries)
        {
            await sqlServer.SqlQueryAsync("ignored", "ignored", true, query);
        }
        sequential.Stop();

        var parallel = Stopwatch.StartNew();
        await sqlServer.RunQueriesInParallel(queries, "ignored", "ignored", true);
        parallel.Stop();

        Assert.True(parallel.Elapsed < sequential.Elapsed);
    }

    private class CaptureParametersSqlServer : DBAClientX.SqlServer
    {
        public List<(string Name, object? Value)> Captured { get; } = new();

        protected override void AddParameters(SqlCommand command, IDictionary<string, object?>? parameters)
        {
            base.AddParameters(command, parameters);
            foreach (SqlParameter p in command.Parameters)
            {
                Captured.Add((p.ParameterName, p.Value));
            }
        }

        public override Task<object?> SqlQueryAsync(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false)
        {
            var command = new SqlCommand(query);
            AddParameters(command, parameters);
            return Task.FromResult<object?>(null);
        }
    }

    [Fact]
    public async Task SqlQueryAsync_BindsParameters()
    {
        var sqlServer = new CaptureParametersSqlServer();
        var parameters = new Dictionary<string, object?>
        {
            ["@id"] = 5,
            ["@name"] = "test"
        };

        await sqlServer.SqlQueryAsync("ignored", "ignored", true, "SELECT 1", parameters);

        Assert.Contains(sqlServer.Captured, p => p.Name == "@id" && (int)p.Value == 5);
        Assert.Contains(sqlServer.Captured, p => p.Name == "@name" && (string)p.Value == "test");
    }

    private class FakeTransactionSqlServer : DBAClientX.SqlServer
    {
        public bool TransactionStarted { get; private set; }

        public override void BeginTransaction(string serverOrInstance, string database, bool integratedSecurity)
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

        public override object? SqlQuery(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false)
        {
            if (useTransaction && !TransactionStarted) throw new DBAClientX.DbaTransactionException("Transaction has not been started.");
            return null;
        }

        public override Task<object?> SqlQueryAsync(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false)
        {
            return Task.FromResult<object?>(SqlQuery(serverOrInstance, database, integratedSecurity, query, parameters, useTransaction));
        }
    }

    [Fact]
    public void SqlQuery_WithTransactionNotStarted_Throws()
    {
        var sqlServer = new FakeTransactionSqlServer();
        Assert.Throws<DBAClientX.DbaTransactionException>(() => sqlServer.SqlQuery("s", "db", true, "q", null, true));
    }

    [Fact]
    public void Commit_EndsTransaction()
    {
        var sqlServer = new FakeTransactionSqlServer();
        sqlServer.BeginTransaction("s", "db", true);
        sqlServer.Commit();
        Assert.Throws<DBAClientX.DbaTransactionException>(() => sqlServer.SqlQuery("s", "db", true, "q", null, true));
    }

    [Fact]
    public void Rollback_EndsTransaction()
    {
        var sqlServer = new FakeTransactionSqlServer();
        sqlServer.BeginTransaction("s", "db", true);
        sqlServer.Rollback();
        Assert.Throws<DBAClientX.DbaTransactionException>(() => sqlServer.SqlQuery("s", "db", true, "q", null, true));
    }

    [Fact]
    public void SqlQuery_UsesTransaction_WhenStarted()
    {
        var sqlServer = new FakeTransactionSqlServer();
        sqlServer.BeginTransaction("s", "db", true);
        var ex = Record.Exception(() => sqlServer.SqlQuery("s", "db", true, "q", null, true));
        Assert.Null(ex);
    }
}
