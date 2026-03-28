using System.Data;
using System.Reflection;
using System.Runtime.CompilerServices;
using Xunit;

namespace DbaClientX.Tests;

public class PostgreSqlTransactionTests
{
    private static readonly FieldInfo TransactionField = typeof(DBAClientX.PostgreSql).GetField("_transaction", BindingFlags.Instance | BindingFlags.NonPublic)!;
    private static readonly FieldInfo TransactionConnectionField = typeof(DBAClientX.PostgreSql).GetField("_transactionConnection", BindingFlags.Instance | BindingFlags.NonPublic)!;

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

    [Fact]
    public void RunInTransaction_CommitsAndReturnsResult()
    {
        using var pg = new TestPostgreSql();

        var result = pg.RunInTransaction("h", "d", "u", "p", client =>
        {
            client.Query("h", "d", "u", "p", "q", useTransaction: true);
            return 42;
        });

        Assert.Equal(42, result);
        Assert.Null(pg.Transaction);
    }

    [Fact]
    public void RunInTransaction_WhenOperationFails_RollsBackAndRethrows()
    {
        using var pg = new TestPostgreSql();
        FakeNpgsqlTransaction? txn = null;

        var ex = Assert.Throws<InvalidOperationException>(() =>
            pg.RunInTransaction("h", "d", "u", "p", client =>
            {
                txn = pg.Transaction;
                throw new InvalidOperationException("boom");
            }));

        Assert.Equal("boom", ex.Message);
        Assert.NotNull(txn);
        Assert.True(txn!.RollbackCalled);
        Assert.Null(pg.Transaction);
    }

    private class BeginFailureTransactionPostgreSql : DBAClientX.PostgreSql
    {
        public int DisposeCalls { get; private set; }

        protected override void OpenConnection(Npgsql.NpgsqlConnection connection)
        {
        }

        protected override Npgsql.NpgsqlTransaction BeginDbTransaction(Npgsql.NpgsqlConnection connection, IsolationLevel isolationLevel)
            => throw new InvalidOperationException("boom");

        protected override void DisposeConnection(Npgsql.NpgsqlConnection connection)
            => DisposeCalls++;
    }

    [Fact]
    public void BeginTransaction_WhenBeginDbTransactionFails_DisposesConnectionAndResetsState()
    {
        using var pg = new BeginFailureTransactionPostgreSql();

        Assert.Throws<InvalidOperationException>(() => pg.BeginTransaction("h", "d", "u", "p"));
        Assert.Throws<InvalidOperationException>(() => pg.BeginTransaction("h", "d", "u", "p"));

        Assert.False(pg.IsInTransaction);
        Assert.Equal(2, pg.DisposeCalls);
    }

    private class ThrowingCommitPostgreSql : DBAClientX.PostgreSql
    {
        public int DisposeCalls { get; private set; }

        public void SeedActiveTransaction()
        {
            TransactionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(Npgsql.NpgsqlTransaction)));
            TransactionConnectionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(Npgsql.NpgsqlConnection)));
        }

        protected override void CommitDbTransaction(Npgsql.NpgsqlTransaction transaction)
            => throw new InvalidOperationException("boom");

        protected override void DisposeTransactionResources(Npgsql.NpgsqlTransaction? transaction, Npgsql.NpgsqlConnection? connection)
            => DisposeCalls++;
    }

    private class ThrowingRollbackPostgreSql : DBAClientX.PostgreSql
    {
        public int DisposeCalls { get; private set; }

        public void SeedActiveTransaction()
        {
            TransactionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(Npgsql.NpgsqlTransaction)));
            TransactionConnectionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(Npgsql.NpgsqlConnection)));
        }

        protected override void RollbackDbTransaction(Npgsql.NpgsqlTransaction transaction)
            => throw new InvalidOperationException("boom");

        protected override void DisposeTransactionResources(Npgsql.NpgsqlTransaction? transaction, Npgsql.NpgsqlConnection? connection)
            => DisposeCalls++;
    }

    [Fact]
    public void Commit_WhenProviderThrows_ClearsTransactionState()
    {
        using var pg = new ThrowingCommitPostgreSql();
        pg.SeedActiveTransaction();

        Assert.Throws<InvalidOperationException>(() => pg.Commit());

        Assert.False(pg.IsInTransaction);
        Assert.Equal(1, pg.DisposeCalls);
        Assert.Throws<DBAClientX.DbaTransactionException>(() => pg.Commit());
    }

    [Fact]
    public void Rollback_WhenProviderThrows_ClearsTransactionState()
    {
        using var pg = new ThrowingRollbackPostgreSql();
        pg.SeedActiveTransaction();

        Assert.Throws<InvalidOperationException>(() => pg.Rollback());

        Assert.False(pg.IsInTransaction);
        Assert.Equal(1, pg.DisposeCalls);
        Assert.Throws<DBAClientX.DbaTransactionException>(() => pg.Rollback());
    }

    private sealed class DisposeTrackingPostgreSql : DBAClientX.PostgreSql
    {
        public int RollbackCalls { get; private set; }
        public int TransactionDisposals { get; private set; }
        public int ConnectionDisposals { get; private set; }

        public void SeedActiveTransaction()
        {
            TransactionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(Npgsql.NpgsqlTransaction)));
            TransactionConnectionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(Npgsql.NpgsqlConnection)));
        }

        protected override void TryRollbackDbTransactionOnDispose(Npgsql.NpgsqlTransaction? transaction)
            => RollbackCalls++;

        protected override void DisposeDbTransaction(Npgsql.NpgsqlTransaction transaction)
            => TransactionDisposals++;

        protected override void DisposeConnection(Npgsql.NpgsqlConnection connection)
            => ConnectionDisposals++;
    }

    [Fact]
    public void Dispose_WithActiveTransaction_RollsBackAndCleansUpOnce()
    {
        var pg = new DisposeTrackingPostgreSql();
        pg.SeedActiveTransaction();

        pg.Dispose();
        pg.Dispose();

        Assert.False(pg.IsInTransaction);
        Assert.Equal(1, pg.RollbackCalls);
        Assert.Equal(1, pg.TransactionDisposals);
        Assert.Equal(1, pg.ConnectionDisposals);
    }
}

