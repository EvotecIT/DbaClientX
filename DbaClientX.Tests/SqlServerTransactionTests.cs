using System.Data;
using Xunit;

namespace DbaClientX.Tests;

public class SqlServerTransactionTests
{
    private class FakeSqlConnection
    {
        public bool BeginCalled { get; private set; }
        public FakeSqlTransaction BeginTransaction()
        {
            BeginCalled = true;
            return new FakeSqlTransaction(this);
        }
    }

    private class FakeSqlTransaction
    {
        private readonly FakeSqlConnection _connection;
        public bool CommitCalled { get; private set; }
        public bool RollbackCalled { get; private set; }

        public FakeSqlTransaction(FakeSqlConnection connection)
        {
            _connection = connection;
        }

        public void Commit() => CommitCalled = true;
        public void Rollback() => RollbackCalled = true;
    }

    private class TestSqlServer : DBAClientX.SqlServer
    {
        public FakeSqlConnection? Connection { get; private set; }
        public FakeSqlTransaction? Transaction { get; private set; }

        public override void BeginTransaction(string serverOrInstance, string database, bool integratedSecurity, string? username = null, string? password = null)
        {
            Connection = new FakeSqlConnection();
            Transaction = Connection.BeginTransaction();
        }

        public override void Commit()
        {
            if (Transaction == null)
            {
                throw new DBAClientX.DbaTransactionException("No active transaction.");
            }
            Transaction.Commit();
            Transaction = null;
        }

        public override void Rollback()
        {
            if (Transaction == null)
            {
                throw new DBAClientX.DbaTransactionException("No active transaction.");
            }
            Transaction.Rollback();
            Transaction = null;
        }

        public override object? Query(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, SqlDbType>? parameterTypes = null, string? username = null, string? password = null)
        {
            if (useTransaction && Transaction == null)
            {
                throw new DBAClientX.DbaTransactionException("Transaction has not been started.");
            }
            return null;
        }
    }

    [Fact]
    public void BeginTransaction_UsesConnection()
    {
        using var server = new TestSqlServer();
        server.BeginTransaction("s", "db", true);
        Assert.NotNull(server.Connection);
        Assert.True(server.Connection!.BeginCalled);
        Assert.NotNull(server.Transaction);
    }

    [Fact]
    public void Commit_CallsCommitOnTransaction()
    {
        using var server = new TestSqlServer();
        server.BeginTransaction("s", "db", true);
        var txn = server.Transaction!;
        server.Commit();
        Assert.True(txn.CommitCalled);
        Assert.Null(server.Transaction);
    }

    [Fact]
    public void Rollback_CallsRollbackOnTransaction()
    {
        using var server = new TestSqlServer();
        server.BeginTransaction("s", "db", true);
        var txn = server.Transaction!;
        server.Rollback();
        Assert.True(txn.RollbackCalled);
        Assert.Null(server.Transaction);
    }
}
