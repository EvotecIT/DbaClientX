using DBAClientX;
using Oracle.ManagedDataAccess.Client;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace DbaClientX.Tests;

public class OracleTests
{
    private class PingOracle : DBAClientX.Oracle
    {
        public bool ShouldFail { get; set; }

        public override object? ExecuteScalar(string host, string serviceName, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, OracleDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            if (ShouldFail) throw new DBAClientX.DbaQueryExecutionException("fail", query, new Exception());
            return 1;
        }

        public override Task<object?> ExecuteScalarAsync(string host, string serviceName, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, OracleDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            if (ShouldFail) throw new DBAClientX.DbaQueryExecutionException("fail", query, new Exception());
            return Task.FromResult<object?>(1);
        }
    }

    [Fact]
    public void Ping_ReturnsTrue_OnSuccess()
    {
        using var oracle = new PingOracle { ShouldFail = false };
        Assert.True(oracle.Ping("h", "svc", "u", "p"));
    }

    [Fact]
    public void Ping_ReturnsFalse_OnFailure()
    {
        using var oracle = new PingOracle { ShouldFail = true };
        Assert.False(oracle.Ping("h", "svc", "u", "p"));
    }

    [Fact]
    public async Task PingAsync_ReturnsTrue_OnSuccess()
    {
        using var oracle = new PingOracle { ShouldFail = false };
        Assert.True(await oracle.PingAsync("h", "svc", "u", "p"));
    }

    [Fact]
    public async Task PingAsync_ReturnsFalse_OnFailure()
    {
        using var oracle = new PingOracle { ShouldFail = true };
        Assert.False(await oracle.PingAsync("h", "svc", "u", "p"));
    }

    private class FakeTransactionOracle : DBAClientX.Oracle
    {
        public bool TransactionStarted { get; private set; }

        public override void BeginTransaction(string host, string serviceName, string username, string password)
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

        public override object? Query(string host, string serviceName, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, OracleDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            if (useTransaction && !TransactionStarted) throw new DBAClientX.DbaTransactionException("Transaction has not been started.");
            return null;
        }

        public override Task<object?> QueryAsync(string host, string serviceName, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, OracleDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            return Task.FromResult<object?>(Query(host, serviceName, username, password, query, parameters, useTransaction, parameterTypes, parameterDirections));
        }
    }

    [Fact]
    public void Query_WithTransactionNotStarted_Throws()
    {
        using var oracle = new FakeTransactionOracle();
        Assert.Throws<DBAClientX.DbaTransactionException>(() => oracle.Query("h", "svc", "u", "p", "q", null, true));
    }

    [Fact]
    public void Commit_WithoutTransaction_Throws()
    {
        using var oracle = new DBAClientX.Oracle();
        Assert.Throws<DBAClientX.DbaTransactionException>(() => oracle.Commit());
    }

    [Fact]
    public void Rollback_WithoutTransaction_Throws()
    {
        using var oracle = new DBAClientX.Oracle();
        Assert.Throws<DBAClientX.DbaTransactionException>(() => oracle.Rollback());
    }

    [Fact]
    public void Commit_EndsTransaction()
    {
        using var oracle = new FakeTransactionOracle();
        oracle.BeginTransaction("h", "svc", "u", "p");
        oracle.Commit();
        Assert.Throws<DBAClientX.DbaTransactionException>(() => oracle.Query("h", "svc", "u", "p", "q", null, true));
    }

    [Fact]
    public void Rollback_EndsTransaction()
    {
        using var oracle = new FakeTransactionOracle();
        oracle.BeginTransaction("h", "svc", "u", "p");
        oracle.Rollback();
        Assert.Throws<DBAClientX.DbaTransactionException>(() => oracle.Query("h", "svc", "u", "p", "q", null, true));
    }

    [Fact]
    public void Query_UsesTransaction_WhenStarted()
    {
        using var oracle = new FakeTransactionOracle();
        oracle.BeginTransaction("h", "svc", "u", "p");
        var ex = Record.Exception(() => oracle.Query("h", "svc", "u", "p", "q", null, true));
        Assert.Null(ex);
    }
}
