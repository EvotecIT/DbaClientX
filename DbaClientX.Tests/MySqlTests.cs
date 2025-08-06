using MySqlConnector;
using System.Data.Common;
using System.Diagnostics;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using System.Data;
using Xunit;

namespace DbaClientX.Tests;

public class MySqlTests
{
    [Fact]
    public async Task MySqlQueryAsync_InvalidServer_ThrowsDbaQueryExecutionException()
    {
        var mySql = new DBAClientX.MySql();
        var ex = await Assert.ThrowsAsync<DBAClientX.DbaQueryExecutionException>(async () =>
        {
            await mySql.MySqlQueryAsync("invalid", "mysql", "user", "pass", "SELECT 1");
        });
        Assert.Contains("SELECT 1", ex.Message);
    }

    private class DelayMySql : DBAClientX.MySql
    {
        private readonly TimeSpan _delay;

        public DelayMySql(TimeSpan delay)
        {
            _delay = delay;
        }

        public override async Task<object?> MySqlQueryAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, MySqlDbType>? parameterTypes = null)
        {
            await Task.Delay(_delay, cancellationToken);
            return null;
        }
    }

    [Fact]
    public async Task RunQueriesInParallel_ExecutesConcurrently()
    {
        var queries = Enumerable.Repeat("SELECT 1", 3).ToArray();
        var mySql = new DelayMySql(TimeSpan.FromMilliseconds(200));

        var sequential = Stopwatch.StartNew();
        foreach (var query in queries)
        {
            await mySql.MySqlQueryAsync("h", "d", "u", "p", query);
        }
        sequential.Stop();

        var parallel = Stopwatch.StartNew();
        await mySql.RunQueriesInParallel(queries, "h", "d", "u", "p");
        parallel.Stop();

        Assert.True(parallel.Elapsed < sequential.Elapsed);
    }

    [Fact]
    public async Task MySqlQueryAsync_CanBeCancelled()
    {
        var mySql = new DelayMySql(TimeSpan.FromSeconds(5));
        using var cts = new CancellationTokenSource(100);
        await Assert.ThrowsAsync<TaskCanceledException>(async () =>
        {
            await mySql.MySqlQueryAsync("h", "d", "u", "p", "q", cancellationToken: cts.Token);
        });
    }

    [Fact]
    public async Task RunQueriesInParallel_ForwardsCancellation()
    {
        var mySql = new DelayMySql(TimeSpan.FromSeconds(5));
        var queries = new[] { "q1", "q2" };
        using var cts = new CancellationTokenSource(100);
        await Assert.ThrowsAsync<TaskCanceledException>(async () =>
        {
            await mySql.RunQueriesInParallel(queries, "h", "d", "u", "p", cts.Token);
        });
    }

    private class CaptureParametersMySql : DBAClientX.MySql
    {
        public List<(string Name, object? Value, MySqlDbType Type)> Captured { get; } = new();

        protected override void AddParameters(DbCommand command, IDictionary<string, object?>? parameters, IDictionary<string, DbType>? parameterTypes = null)
        {
            base.AddParameters(command, parameters, parameterTypes);
            foreach (DbParameter p in command.Parameters)
            {
                if (p is MySqlParameter mp)
                {
                    Captured.Add((mp.ParameterName, mp.Value, mp.MySqlDbType));
                }
            }
        }

        public override Task<object?> MySqlQueryAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, MySqlDbType>? parameterTypes = null)
        {
            var command = new MySqlCommand(query);
            IDictionary<string, DbType>? dbTypes = null;
            if (parameterTypes != null)
            {
                dbTypes = new Dictionary<string, DbType>(parameterTypes.Count);
                foreach (var kv in parameterTypes)
                {
                    var p = new MySqlParameter { MySqlDbType = kv.Value };
                    dbTypes[kv.Key] = p.DbType;
                }
            }
            AddParameters(command, parameters, dbTypes);
            return Task.FromResult<object?>(null);
        }
    }

    [Fact]
    public async Task MySqlQueryAsync_BindsParameters()
    {
        var mySql = new CaptureParametersMySql();
        var parameters = new Dictionary<string, object?>
        {
            ["@id"] = 5,
            ["@name"] = "test"
        };

        await mySql.MySqlQueryAsync("h", "d", "u", "p", "SELECT 1", parameters);

        Assert.Contains(mySql.Captured, p => p.Name == "@id" && (int)p.Value == 5);
        Assert.Contains(mySql.Captured, p => p.Name == "@name" && (string)p.Value == "test");
    }

    [Fact]
    public async Task MySqlQueryAsync_PreservesParameterTypes()
    {
        var mySql = new CaptureParametersMySql();
        var parameters = new Dictionary<string, object?>
        {
            ["@id"] = 5,
            ["@name"] = "test"
        };
        var types = new Dictionary<string, MySqlDbType>
        {
            ["@id"] = MySqlDbType.Int32,
            ["@name"] = MySqlDbType.VarChar
        };

        await mySql.MySqlQueryAsync("h", "d", "u", "p", "SELECT 1", parameters, cancellationToken: CancellationToken.None, parameterTypes: types);

        Assert.Contains(mySql.Captured, p => p.Name == "@id" && p.Type == MySqlDbType.Int32);
        Assert.Contains(mySql.Captured, p => p.Name == "@name" && p.Type == MySqlDbType.VarChar);
    }

    private class FakeTransactionMySql : DBAClientX.MySql
    {
        public bool TransactionStarted { get; private set; }

        public override void BeginTransaction(string host, string database, string username, string password)
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

        public override object? MySqlQuery(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, MySqlDbType>? parameterTypes = null)
        {
            if (useTransaction && !TransactionStarted) throw new DBAClientX.DbaTransactionException("Transaction has not been started.");
            return null;
        }

        public override Task<object?> MySqlQueryAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, MySqlDbType>? parameterTypes = null)
        {
            return Task.FromResult<object?>(MySqlQuery(host, database, username, password, query, parameters, useTransaction));
        }
    }

    [Fact]
    public void MySqlQuery_WithTransactionNotStarted_Throws()
    {
        var mySql = new FakeTransactionMySql();
        Assert.Throws<DBAClientX.DbaTransactionException>(() => mySql.MySqlQuery("h", "d", "u", "p", "q", null, true));
    }

    [Fact]
    public void Commit_EndsTransaction()
    {
        var mySql = new FakeTransactionMySql();
        mySql.BeginTransaction("h", "d", "u", "p");
        mySql.Commit();
        Assert.Throws<DBAClientX.DbaTransactionException>(() => mySql.MySqlQuery("h", "d", "u", "p", "q", null, true));
    }

    [Fact]
    public void Rollback_EndsTransaction()
    {
        var mySql = new FakeTransactionMySql();
        mySql.BeginTransaction("h", "d", "u", "p");
        mySql.Rollback();
        Assert.Throws<DBAClientX.DbaTransactionException>(() => mySql.MySqlQuery("h", "d", "u", "p", "q", null, true));
    }

    [Fact]
    public void MySqlQuery_UsesTransaction_WhenStarted()
    {
        var mySql = new FakeTransactionMySql();
        mySql.BeginTransaction("h", "d", "u", "p");
        var ex = Record.Exception(() => mySql.MySqlQuery("h", "d", "u", "p", "q", null, true));
        Assert.Null(ex);
    }
}
