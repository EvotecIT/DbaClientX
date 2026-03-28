using System.Data;
using System.Reflection;
using System.Runtime.CompilerServices;
using Oracle.ManagedDataAccess.Client;
using Xunit;

namespace DbaClientX.Tests;

public class OracleTransactionTests
{
    private static readonly FieldInfo TransactionField = typeof(DBAClientX.Oracle).GetField("_transaction", BindingFlags.Instance | BindingFlags.NonPublic)!;
    private static readonly FieldInfo TransactionConnectionField = typeof(DBAClientX.Oracle).GetField("_transactionConnection", BindingFlags.Instance | BindingFlags.NonPublic)!;

    private class FakeOracleConnection
    {
        public bool BeginCalled { get; private set; }
        public IsolationLevel? Level { get; private set; }

        public FakeOracleTransaction BeginTransaction()
            => BeginTransaction(IsolationLevel.ReadCommitted);

        public FakeOracleTransaction BeginTransaction(IsolationLevel isolationLevel)
        {
            BeginCalled = true;
            Level = isolationLevel;
            return new FakeOracleTransaction(this);
        }
    }

    private class FakeOracleTransaction
    {
        private readonly FakeOracleConnection _connection;
        public bool CommitCalled { get; private set; }
        public bool RollbackCalled { get; private set; }

        public FakeOracleTransaction(FakeOracleConnection connection)
        {
            _connection = connection;
        }

        public void Commit() => CommitCalled = true;
        public void Rollback() => RollbackCalled = true;
    }

    private class TestOracle : DBAClientX.Oracle
    {
        public FakeOracleConnection? Connection { get; private set; }
        public FakeOracleTransaction? Transaction { get; private set; }

        public override void BeginTransaction(string host, string serviceName, string username, string password)
        {
            Connection = new FakeOracleConnection();
            Transaction = Connection.BeginTransaction();
        }

        public override void BeginTransaction(string host, string serviceName, string username, string password, IsolationLevel isolationLevel)
        {
            Connection = new FakeOracleConnection();
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
    }

    [Fact]
    public void BeginTransaction_UsesConnection()
    {
        using var oracle = new TestOracle();
        oracle.BeginTransaction("h", "svc", "u", "p");
        Assert.NotNull(oracle.Connection);
        Assert.True(oracle.Connection!.BeginCalled);
        Assert.NotNull(oracle.Transaction);
    }

    [Fact]
    public void Commit_CallsCommitOnTransaction()
    {
        using var oracle = new TestOracle();
        oracle.BeginTransaction("h", "svc", "u", "p");
        var transaction = oracle.Transaction!;
        oracle.Commit();
        Assert.True(transaction.CommitCalled);
        Assert.Null(oracle.Transaction);
    }

    [Fact]
    public void Rollback_CallsRollbackOnTransaction()
    {
        using var oracle = new TestOracle();
        oracle.BeginTransaction("h", "svc", "u", "p");
        var transaction = oracle.Transaction!;
        oracle.Rollback();
        Assert.True(transaction.RollbackCalled);
        Assert.Null(oracle.Transaction);
    }

    [Fact]
    public void BeginTransaction_WithIsolationLevel_PassesIsolationLevel()
    {
        using var oracle = new TestOracle();
        oracle.BeginTransaction("h", "svc", "u", "p", IsolationLevel.Serializable);
        Assert.NotNull(oracle.Connection);
        Assert.Equal(IsolationLevel.Serializable, oracle.Connection!.Level);
    }

    [Fact]
    public void RunInTransaction_CommitsAndReturnsResult()
    {
        using var oracle = new TestOracle();

        var result = oracle.RunInTransaction("h", "svc", "u", "p", client =>
        {
            return 42;
        });

        Assert.Equal(42, result);
        Assert.Null(oracle.Transaction);
    }

    [Fact]
    public void RunInTransaction_WhenOperationFails_RollsBackAndRethrows()
    {
        using var oracle = new TestOracle();
        FakeOracleTransaction? txn = null;

        var ex = Assert.Throws<InvalidOperationException>(() =>
            oracle.RunInTransaction("h", "svc", "u", "p", client =>
            {
                txn = oracle.Transaction;
                throw new InvalidOperationException("boom");
            }));

        Assert.Equal("boom", ex.Message);
        Assert.NotNull(txn);
        Assert.True(txn!.RollbackCalled);
        Assert.Null(oracle.Transaction);
    }

    private class BeginFailureTransactionOracle : DBAClientX.Oracle
    {
        public int DisposeCalls { get; private set; }

        protected override void OpenConnection(OracleConnection connection)
        {
        }

        protected override OracleTransaction BeginDbTransaction(OracleConnection connection, IsolationLevel isolationLevel)
            => throw new InvalidOperationException("boom");

        protected override void DisposeConnection(OracleConnection connection)
            => DisposeCalls++;
    }

    [Fact]
    public void BeginTransaction_WhenBeginDbTransactionFails_DisposesConnectionAndResetsState()
    {
        using var oracle = new BeginFailureTransactionOracle();

        Assert.Throws<InvalidOperationException>(() => oracle.BeginTransaction("h", "svc", "u", "p"));
        Assert.Throws<InvalidOperationException>(() => oracle.BeginTransaction("h", "svc", "u", "p"));

        Assert.False(oracle.IsInTransaction);
        Assert.Equal(2, oracle.DisposeCalls);
    }

    private class ThrowingCommitOracle : DBAClientX.Oracle
    {
        public int DisposeCalls { get; private set; }

        public void SeedActiveTransaction()
        {
            TransactionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(OracleTransaction)));
            TransactionConnectionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(OracleConnection)));
        }

        protected override void CommitDbTransaction(OracleTransaction transaction)
            => throw new InvalidOperationException("boom");

        protected override void DisposeTransactionResources(OracleTransaction? transaction, OracleConnection? connection)
            => DisposeCalls++;
    }

    private class ThrowingRollbackOracle : DBAClientX.Oracle
    {
        public int DisposeCalls { get; private set; }

        public void SeedActiveTransaction()
        {
            TransactionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(OracleTransaction)));
            TransactionConnectionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(OracleConnection)));
        }

        protected override void RollbackDbTransaction(OracleTransaction transaction)
            => throw new InvalidOperationException("boom");

        protected override void DisposeTransactionResources(OracleTransaction? transaction, OracleConnection? connection)
            => DisposeCalls++;
    }

    [Fact]
    public void Commit_WhenProviderThrows_ClearsTransactionState()
    {
        using var oracle = new ThrowingCommitOracle();
        oracle.SeedActiveTransaction();

        Assert.Throws<InvalidOperationException>(() => oracle.Commit());

        Assert.False(oracle.IsInTransaction);
        Assert.Equal(1, oracle.DisposeCalls);
        Assert.Throws<DBAClientX.DbaTransactionException>(() => oracle.Commit());
    }

    [Fact]
    public void Rollback_WhenProviderThrows_ClearsTransactionState()
    {
        using var oracle = new ThrowingRollbackOracle();
        oracle.SeedActiveTransaction();

        Assert.Throws<InvalidOperationException>(() => oracle.Rollback());

        Assert.False(oracle.IsInTransaction);
        Assert.Equal(1, oracle.DisposeCalls);
        Assert.Throws<DBAClientX.DbaTransactionException>(() => oracle.Rollback());
    }

    private sealed class DisposeTrackingOracle : DBAClientX.Oracle
    {
        public int RollbackCalls { get; private set; }
        public int TransactionDisposals { get; private set; }
        public int ConnectionDisposals { get; private set; }

        public void SeedActiveTransaction()
        {
            TransactionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(OracleTransaction)));
            TransactionConnectionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(OracleConnection)));
        }

        protected override void TryRollbackDbTransactionOnDispose(OracleTransaction? transaction)
            => RollbackCalls++;

        protected override void DisposeDbTransaction(OracleTransaction transaction)
            => TransactionDisposals++;

        protected override void DisposeConnection(OracleConnection connection)
            => ConnectionDisposals++;
    }

    [Fact]
    public void Dispose_WithActiveTransaction_RollsBackAndCleansUpOnce()
    {
        var oracle = new DisposeTrackingOracle();
        oracle.SeedActiveTransaction();

        oracle.Dispose();
        oracle.Dispose();

        Assert.False(oracle.IsInTransaction);
        Assert.Equal(1, oracle.RollbackCalls);
        Assert.Equal(1, oracle.TransactionDisposals);
        Assert.Equal(1, oracle.ConnectionDisposals);
    }
}
