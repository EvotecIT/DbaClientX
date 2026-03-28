using System.Collections.Generic;
using System.Data;
using System.Reflection;
using System.Runtime.CompilerServices;
using MySqlConnector;
using Xunit;

namespace DbaClientX.Tests;

public class MySqlTransactionTests
{
    private static readonly FieldInfo TransactionField = typeof(DBAClientX.MySql).GetField("_transaction", BindingFlags.Instance | BindingFlags.NonPublic)!;
    private static readonly FieldInfo TransactionConnectionField = typeof(DBAClientX.MySql).GetField("_transactionConnection", BindingFlags.Instance | BindingFlags.NonPublic)!;

    private class FakeMySqlConnection
    {
        public bool BeginCalled { get; private set; }
        public IsolationLevel? Level { get; private set; }

        public FakeMySqlTransaction BeginTransaction() => BeginTransaction(IsolationLevel.ReadCommitted);

        public FakeMySqlTransaction BeginTransaction(IsolationLevel isolationLevel)
        {
            BeginCalled = true;
            Level = isolationLevel;
            return new FakeMySqlTransaction(this);
        }
    }

    private class FakeMySqlTransaction
    {
        private readonly FakeMySqlConnection _connection;
        public bool CommitCalled { get; private set; }
        public bool RollbackCalled { get; private set; }

        public FakeMySqlTransaction(FakeMySqlConnection connection) => _connection = connection;

        public void Commit() => CommitCalled = true;
        public void Rollback() => RollbackCalled = true;
    }

    private class TestMySql : DBAClientX.MySql
    {
        public FakeMySqlConnection? Connection { get; private set; }
        public FakeMySqlTransaction? Transaction { get; private set; }

        public override void BeginTransaction(string host, string database, string username, string password)
        {
            Connection = new FakeMySqlConnection();
            Transaction = Connection.BeginTransaction();
        }

        public override void BeginTransaction(string host, string database, string username, string password, IsolationLevel isolationLevel)
        {
            Connection = new FakeMySqlConnection();
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

        public override object? Query(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, MySqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
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
        using var mySql = new TestMySql();
        mySql.BeginTransaction("h", "d", "u", "p");
        Assert.NotNull(mySql.Connection);
        Assert.True(mySql.Connection!.BeginCalled);
        Assert.NotNull(mySql.Transaction);
    }

    [Fact]
    public void Commit_CallsCommitOnTransaction()
    {
        using var mySql = new TestMySql();
        mySql.BeginTransaction("h", "d", "u", "p");
        var txn = mySql.Transaction!;
        mySql.Commit();
        Assert.True(txn.CommitCalled);
        Assert.Null(mySql.Transaction);
    }

    [Fact]
    public void Rollback_CallsRollbackOnTransaction()
    {
        using var mySql = new TestMySql();
        mySql.BeginTransaction("h", "d", "u", "p");
        var txn = mySql.Transaction!;
        mySql.Rollback();
        Assert.True(txn.RollbackCalled);
        Assert.Null(mySql.Transaction);
    }

    [Fact]
    public void BeginTransaction_WithIsolationLevel_PassesIsolationLevel()
    {
        using var mySql = new TestMySql();
        mySql.BeginTransaction("h", "d", "u", "p", IsolationLevel.Serializable);
        Assert.NotNull(mySql.Connection);
        Assert.Equal(IsolationLevel.Serializable, mySql.Connection!.Level);
    }

    [Fact]
    public void RunInTransaction_CommitsAndReturnsResult()
    {
        using var mySql = new TestMySql();

        var result = mySql.RunInTransaction("h", "d", "u", "p", client =>
        {
            client.Query("h", "d", "u", "p", "q", useTransaction: true);
            return 42;
        });

        Assert.Equal(42, result);
        Assert.Null(mySql.Transaction);
    }

    [Fact]
    public void RunInTransaction_WhenOperationFails_RollsBackAndRethrows()
    {
        using var mySql = new TestMySql();
        FakeMySqlTransaction? txn = null;

        var ex = Assert.Throws<InvalidOperationException>(() =>
            mySql.RunInTransaction("h", "d", "u", "p", client =>
            {
                txn = mySql.Transaction;
                throw new InvalidOperationException("boom");
            }));

        Assert.Equal("boom", ex.Message);
        Assert.NotNull(txn);
        Assert.True(txn!.RollbackCalled);
        Assert.Null(mySql.Transaction);
    }

    private class BeginFailureTransactionMySql : DBAClientX.MySql
    {
        public int DisposeCalls { get; private set; }

        protected override void OpenConnection(MySqlConnection connection)
        {
        }

        protected override MySqlTransaction BeginDbTransaction(MySqlConnection connection, IsolationLevel isolationLevel)
            => throw new InvalidOperationException("boom");

        protected override void DisposeConnection(MySqlConnection connection)
            => DisposeCalls++;
    }

    [Fact]
    public void BeginTransaction_WhenBeginDbTransactionFails_DisposesConnectionAndResetsState()
    {
        using var mySql = new BeginFailureTransactionMySql();

        Assert.Throws<InvalidOperationException>(() => mySql.BeginTransaction("h", "d", "u", "p"));
        Assert.Throws<InvalidOperationException>(() => mySql.BeginTransaction("h", "d", "u", "p"));

        Assert.False(mySql.IsInTransaction);
        Assert.Equal(2, mySql.DisposeCalls);
    }

    private class ThrowingCommitMySql : DBAClientX.MySql
    {
        public int DisposeCalls { get; private set; }

        public void SeedActiveTransaction()
        {
            TransactionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(MySqlTransaction)));
            TransactionConnectionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(MySqlConnection)));
        }

        protected override void CommitDbTransaction(MySqlTransaction transaction)
            => throw new InvalidOperationException("boom");

        protected override void DisposeTransactionResources(MySqlTransaction? transaction, MySqlConnection? connection)
            => DisposeCalls++;
    }

    private class ThrowingRollbackMySql : DBAClientX.MySql
    {
        public int DisposeCalls { get; private set; }

        public void SeedActiveTransaction()
        {
            TransactionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(MySqlTransaction)));
            TransactionConnectionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(MySqlConnection)));
        }

        protected override void RollbackDbTransaction(MySqlTransaction transaction)
            => throw new InvalidOperationException("boom");

        protected override void DisposeTransactionResources(MySqlTransaction? transaction, MySqlConnection? connection)
            => DisposeCalls++;
    }

    [Fact]
    public void Commit_WhenProviderThrows_ClearsTransactionState()
    {
        using var mySql = new ThrowingCommitMySql();
        mySql.SeedActiveTransaction();

        Assert.Throws<InvalidOperationException>(() => mySql.Commit());

        Assert.False(mySql.IsInTransaction);
        Assert.Equal(1, mySql.DisposeCalls);
        Assert.Throws<DBAClientX.DbaTransactionException>(() => mySql.Commit());
    }

    [Fact]
    public void Rollback_WhenProviderThrows_ClearsTransactionState()
    {
        using var mySql = new ThrowingRollbackMySql();
        mySql.SeedActiveTransaction();

        Assert.Throws<InvalidOperationException>(() => mySql.Rollback());

        Assert.False(mySql.IsInTransaction);
        Assert.Equal(1, mySql.DisposeCalls);
        Assert.Throws<DBAClientX.DbaTransactionException>(() => mySql.Rollback());
    }

    private sealed class DisposeTrackingMySql : DBAClientX.MySql
    {
        public int RollbackCalls { get; private set; }
        public int TransactionDisposals { get; private set; }
        public int ConnectionDisposals { get; private set; }

        public void SeedActiveTransaction()
        {
            TransactionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(MySqlTransaction)));
            TransactionConnectionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(MySqlConnection)));
        }

        protected override void TryRollbackDbTransactionOnDispose(MySqlTransaction? transaction)
            => RollbackCalls++;

        protected override void DisposeDbTransaction(MySqlTransaction transaction)
            => TransactionDisposals++;

        protected override void DisposeConnection(MySqlConnection connection)
            => ConnectionDisposals++;
    }

    [Fact]
    public void Dispose_WithActiveTransaction_RollsBackAndCleansUpOnce()
    {
        var mySql = new DisposeTrackingMySql();
        mySql.SeedActiveTransaction();

        mySql.Dispose();
        mySql.Dispose();

        Assert.False(mySql.IsInTransaction);
        Assert.Equal(1, mySql.RollbackCalls);
        Assert.Equal(1, mySql.TransactionDisposals);
        Assert.Equal(1, mySql.ConnectionDisposals);
    }
}
