using System.Data;
using System.Reflection;
using System.Runtime.CompilerServices;
using Microsoft.Data.SqlClient;
using Xunit;

namespace DbaClientX.Tests;

public class SqlServerTransactionTests
{
    private static readonly FieldInfo TransactionField = typeof(DBAClientX.SqlServer).GetField("_transaction", BindingFlags.Instance | BindingFlags.NonPublic)!;
    private static readonly FieldInfo TransactionConnectionField = typeof(DBAClientX.SqlServer).GetField("_transactionConnection", BindingFlags.Instance | BindingFlags.NonPublic)!;

    private class FakeSqlConnection
    {
        public bool BeginCalled { get; private set; }
        public IsolationLevel? Level { get; private set; }

        public FakeSqlTransaction BeginTransaction()
            => BeginTransaction(IsolationLevel.ReadCommitted);

        public FakeSqlTransaction BeginTransaction(IsolationLevel isolationLevel)
        {
            BeginCalled = true;
            Level = isolationLevel;
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

        public override void BeginTransaction(string serverOrInstance, string database, bool integratedSecurity, IsolationLevel isolationLevel, string? username = null, string? password = null)
        {
            Connection = new FakeSqlConnection();
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

        public override object? Query(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, SqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null, string? username = null, string? password = null)
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

    [Fact]
    public void BeginTransaction_WithIsolationLevel_PassesIsolationLevel()
    {
        using var server = new TestSqlServer();
        server.BeginTransaction("s", "db", true, IsolationLevel.Serializable);
        Assert.NotNull(server.Connection);
        Assert.Equal(IsolationLevel.Serializable, server.Connection!.Level);
    }

    [Fact]
    public void RunInTransaction_CommitsAndReturnsResult()
    {
        using var server = new TestSqlServer();

        var result = server.RunInTransaction("s", "db", true, client =>
        {
            client.Query("s", "db", true, "q", useTransaction: true);
            return 42;
        });

        Assert.Equal(42, result);
        Assert.Null(server.Transaction);
    }

    [Fact]
    public void RunInTransaction_WhenOperationFails_RollsBackAndRethrows()
    {
        using var server = new TestSqlServer();
        FakeSqlTransaction? txn = null;

        var ex = Assert.Throws<InvalidOperationException>(() =>
            server.RunInTransaction("s", "db", true, client =>
            {
                txn = server.Transaction;
                throw new InvalidOperationException("boom");
            }));

        Assert.Equal("boom", ex.Message);
        Assert.NotNull(txn);
        Assert.True(txn!.RollbackCalled);
        Assert.Null(server.Transaction);
    }

    private class BeginFailureTransactionSqlServer : DBAClientX.SqlServer
    {
        public int DisposeCalls { get; private set; }

        protected override void OpenConnection(SqlConnection connection)
        {
        }

        protected override SqlTransaction BeginDbTransaction(SqlConnection connection, IsolationLevel isolationLevel)
            => throw new InvalidOperationException("boom");

        protected override void DisposeConnection(SqlConnection connection)
            => DisposeCalls++;
    }

    [Fact]
    public void BeginTransaction_WhenBeginDbTransactionFails_DisposesConnectionAndResetsState()
    {
        using var server = new BeginFailureTransactionSqlServer();

        Assert.Throws<InvalidOperationException>(() => server.BeginTransaction("s", "db", true));
        Assert.Throws<InvalidOperationException>(() => server.BeginTransaction("s", "db", true));

        Assert.False(server.IsInTransaction);
        Assert.Equal(2, server.DisposeCalls);
    }

    private class ThrowingCommitSqlServer : DBAClientX.SqlServer
    {
        public int DisposeCalls { get; private set; }

        public void SeedActiveTransaction()
        {
            TransactionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(SqlTransaction)));
            TransactionConnectionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(SqlConnection)));
        }

        protected override void CommitDbTransaction(SqlTransaction transaction)
            => throw new InvalidOperationException("boom");

        protected override void DisposeTransactionResources(SqlTransaction? transaction, SqlConnection? connection)
            => DisposeCalls++;
    }

    private class ThrowingRollbackSqlServer : DBAClientX.SqlServer
    {
        public int DisposeCalls { get; private set; }

        public void SeedActiveTransaction()
        {
            TransactionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(SqlTransaction)));
            TransactionConnectionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(SqlConnection)));
        }

        protected override void RollbackDbTransaction(SqlTransaction transaction)
            => throw new InvalidOperationException("boom");

        protected override void DisposeTransactionResources(SqlTransaction? transaction, SqlConnection? connection)
            => DisposeCalls++;
    }

    [Fact]
    public void Commit_WhenProviderThrows_ClearsTransactionState()
    {
        using var server = new ThrowingCommitSqlServer();
        server.SeedActiveTransaction();

        Assert.Throws<InvalidOperationException>(() => server.Commit());

        Assert.False(server.IsInTransaction);
        Assert.Equal(1, server.DisposeCalls);
        Assert.Throws<DBAClientX.DbaTransactionException>(() => server.Commit());
    }

    [Fact]
    public void Rollback_WhenProviderThrows_ClearsTransactionState()
    {
        using var server = new ThrowingRollbackSqlServer();
        server.SeedActiveTransaction();

        Assert.Throws<InvalidOperationException>(() => server.Rollback());

        Assert.False(server.IsInTransaction);
        Assert.Equal(1, server.DisposeCalls);
        Assert.Throws<DBAClientX.DbaTransactionException>(() => server.Rollback());
    }

    private sealed class DisposeTrackingSqlServer : DBAClientX.SqlServer
    {
        public int RollbackCalls { get; private set; }
        public int TransactionDisposals { get; private set; }
        public int ConnectionDisposals { get; private set; }

        public void SeedActiveTransaction()
        {
            TransactionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(SqlTransaction)));
            TransactionConnectionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(SqlConnection)));
        }

        protected override void TryRollbackDbTransactionOnDispose(SqlTransaction? transaction)
            => RollbackCalls++;

        protected override void DisposeDbTransaction(SqlTransaction transaction)
            => TransactionDisposals++;

        protected override void DisposeConnection(SqlConnection connection)
            => ConnectionDisposals++;
    }

    [Fact]
    public void Dispose_WithActiveTransaction_RollsBackAndCleansUpOnce()
    {
        var server = new DisposeTrackingSqlServer();
        server.SeedActiveTransaction();

        server.Dispose();
        server.Dispose();

        Assert.False(server.IsInTransaction);
        Assert.Equal(1, server.RollbackCalls);
        Assert.Equal(1, server.TransactionDisposals);
        Assert.Equal(1, server.ConnectionDisposals);
    }
}
