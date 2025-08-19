using System.Data;
using Xunit;

namespace DbaClientX.Tests;

public class PostgreSqlTransactionTests
{
    private class FakeNpgsqlConnection
    {
        public bool BeginCalled { get; private set; }
        public IsolationLevel? Level { get; private set; }

        public FakeNpgsqlTransaction BeginTransaction()
            => BeginTransaction(IsolationLevel.ReadCommitted);

        public FakeNpgsqlTransaction BeginTransaction(IsolationLevel isolationLevel)
        {
            BeginCalled = true;
            Level = isolationLevel;
            return new FakeNpgsqlTransaction(this);
        }
    }

    private class FakeNpgsqlTransaction
    {
        private readonly FakeNpgsqlConnection _connection;
        public bool CommitCalled { get; private set; }
        public bool RollbackCalled { get; private set; }

        public FakeNpgsqlTransaction(FakeNpgsqlConnection connection)
        {
            _connection = connection;
        }

        public void Commit() => CommitCalled = true;
        public void Rollback() => RollbackCalled = true;
    }

    private class TestPostgreSql : DBAClientX.PostgreSql
    {
        public FakeNpgsqlConnection? Connection { get; private set; }
        public FakeNpgsqlTransaction? Transaction { get; private set; }

        public override void BeginTransaction(string host, string database, string username, string password)
        {
            Connection = new FakeNpgsqlConnection();
            Transaction = Connection.BeginTransaction();
        }

        public override void BeginTransaction(string host, string database, string username, string password, IsolationLevel isolationLevel)
        {
            Connection = new FakeNpgsqlConnection();
            Transaction = Connection.BeginTransaction(isolationLevel);
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

        public override object? Query(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, NpgsqlTypes.NpgsqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
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
        using var pg = new TestPostgreSql();
        pg.BeginTransaction("h", "d", "u", "p");
        Assert.NotNull(pg.Connection);
        Assert.True(pg.Connection!.BeginCalled);
        Assert.NotNull(pg.Transaction);
    }

    [Fact]
    public void Commit_CallsCommitOnTransaction()
    {
        using var pg = new TestPostgreSql();
        pg.BeginTransaction("h", "d", "u", "p");
        var txn = pg.Transaction!;
        pg.Commit();
        Assert.True(txn.CommitCalled);
        Assert.Null(pg.Transaction);
    }

    [Fact]
    public void Rollback_CallsRollbackOnTransaction()
    {
        using var pg = new TestPostgreSql();
        pg.BeginTransaction("h", "d", "u", "p");
        var txn = pg.Transaction!;
        pg.Rollback();
        Assert.True(txn.RollbackCalled);
        Assert.Null(pg.Transaction);
    }

    [Fact]
    public void BeginTransaction_WithIsolationLevel_PassesIsolationLevel()
    {
        using var pg = new TestPostgreSql();
        pg.BeginTransaction("h", "d", "u", "p", IsolationLevel.Serializable);
        Assert.NotNull(pg.Connection);
        Assert.Equal(IsolationLevel.Serializable, pg.Connection!.Level);
    }
}

