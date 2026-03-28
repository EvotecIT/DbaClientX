using System.Data;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Xunit;

namespace DbaClientX.Tests;

public class SqlServerTransactionAsyncTests
{
    private static readonly FieldInfo TransactionField = typeof(DBAClientX.SqlServer).GetField("_transaction", BindingFlags.Instance | BindingFlags.NonPublic)!;
    private static readonly FieldInfo TransactionConnectionField = typeof(DBAClientX.SqlServer).GetField("_transactionConnection", BindingFlags.Instance | BindingFlags.NonPublic)!;

    private class FakeSqlConnection
    {
        public bool BeginCalled { get; private set; }
        public IsolationLevel? Level { get; private set; }

        public Task<FakeSqlTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
            => BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);

        public async Task<FakeSqlTransaction> BeginTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken = default)
        {
            BeginCalled = true;
            Level = isolationLevel;
            await Task.Yield();
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

        public Task CommitAsync(CancellationToken cancellationToken = default)
        {
            CommitCalled = true;
            return Task.CompletedTask;
        }

        public Task RollbackAsync(CancellationToken cancellationToken = default)
        {
            RollbackCalled = true;
            return Task.CompletedTask;
        }
    }

    private class TestSqlServer : DBAClientX.SqlServer
    {
        private readonly object _syncRoot = new();
        public FakeSqlConnection? Connection { get; private set; }
        public FakeSqlTransaction? Transaction { get; private set; }

        public override async Task BeginTransactionAsync(string serverOrInstance, string database, bool integratedSecurity, CancellationToken cancellationToken = default, string? username = null, string? password = null)
        {
            lock (_syncRoot)
            {
                if (Transaction != null)
                {
                    throw new DBAClientX.DbaTransactionException("Transaction already started.");
                }
            }

            var connection = new FakeSqlConnection();
            var transaction = await connection.BeginTransactionAsync(cancellationToken);

            lock (_syncRoot)
            {
                if (Transaction != null)
                {
                    throw new DBAClientX.DbaTransactionException("Transaction already started.");
                }

                Connection = connection;
                Transaction = transaction;
            }
        }

        public override async Task BeginTransactionAsync(string serverOrInstance, string database, bool integratedSecurity, IsolationLevel isolationLevel, CancellationToken cancellationToken = default, string? username = null, string? password = null)
        {
            lock (_syncRoot)
            {
                if (Transaction != null)
                {
                    throw new DBAClientX.DbaTransactionException("Transaction already started.");
                }
            }

            var connection = new FakeSqlConnection();
            var transaction = await connection.BeginTransactionAsync(isolationLevel, cancellationToken);

            lock (_syncRoot)
            {
                if (Transaction != null)
                {
                    throw new DBAClientX.DbaTransactionException("Transaction already started.");
                }

                Connection = connection;
                Transaction = transaction;
            }
        }

        public override async Task CommitAsync(CancellationToken cancellationToken = default)
        {
            if (Transaction == null)
            {
                throw new DBAClientX.DbaTransactionException("No active transaction.");
            }
            await Transaction.CommitAsync(cancellationToken);
            Transaction = null;
        }

        public override async Task RollbackAsync(CancellationToken cancellationToken = default)
        {
            if (Transaction == null)
            {
                throw new DBAClientX.DbaTransactionException("No active transaction.");
            }
            await Transaction.RollbackAsync(cancellationToken);
            Transaction = null;
        }

        public override Task<object?> QueryAsync(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, SqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null, string? username = null, string? password = null)
        {
            if (useTransaction && Transaction == null)
            {
                throw new DBAClientX.DbaTransactionException("Transaction has not been started.");
            }
            return Task.FromResult<object?>(null);
        }
    }

    [Fact]
    public async Task BeginTransactionAsync_UsesConnection()
    {
        using var server = new TestSqlServer();
        await server.BeginTransactionAsync("s", "db", true);
        Assert.NotNull(server.Connection);
        Assert.True(server.Connection!.BeginCalled);
        Assert.NotNull(server.Transaction);
    }

    [Fact]
    public async Task BeginTransactionAsync_WhenCalledConcurrently_Throws()
    {
        using var server = new TestSqlServer();

        var tasks = new[]
        {
            Task.Run(() => server.BeginTransactionAsync("s", "db", true)),
            Task.Run(() => server.BeginTransactionAsync("s", "db", true))
        };

        await Assert.ThrowsAsync<DBAClientX.DbaTransactionException>(() => Task.WhenAll(tasks));
        Assert.NotNull(server.Transaction);
    }

    [Fact]
    public async Task CommitAsync_CallsCommitOnTransaction()
    {
        using var server = new TestSqlServer();
        await server.BeginTransactionAsync("s", "db", true);
        var txn = server.Transaction!;
        await server.CommitAsync();
        Assert.True(txn.CommitCalled);
        Assert.Null(server.Transaction);
    }

    [Fact]
    public async Task RollbackAsync_CallsRollbackOnTransaction()
    {
        using var server = new TestSqlServer();
        await server.BeginTransactionAsync("s", "db", true);
        var txn = server.Transaction!;
        await server.RollbackAsync();
        Assert.True(txn.RollbackCalled);
        Assert.Null(server.Transaction);
    }

    [Fact]
    public async Task BeginTransactionAsync_WithIsolationLevel_PassesIsolationLevel()
    {
        using var server = new TestSqlServer();
        await server.BeginTransactionAsync("s", "db", true, IsolationLevel.Serializable);
        Assert.NotNull(server.Connection);
        Assert.Equal(IsolationLevel.Serializable, server.Connection!.Level);
    }

    [Fact]
    public async Task RunInTransactionAsync_CommitsAndReturnsResult()
    {
        using var server = new TestSqlServer();

        var result = await server.RunInTransactionAsync("s", "db", true, async (client, token) =>
        {
            await client.QueryAsync("s", "db", true, "q", useTransaction: true, cancellationToken: token);
            return 42;
        });

        Assert.Equal(42, result);
        Assert.Null(server.Transaction);
    }

    [Fact]
    public async Task RunInTransactionAsync_WhenOperationFails_RollsBackAndRethrows()
    {
        using var server = new TestSqlServer();
        FakeSqlTransaction? txn = null;

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            server.RunInTransactionAsync("s", "db", true, async (_, _) =>
            {
                txn = server.Transaction;
                await Task.Yield();
                throw new InvalidOperationException("boom");
            }));

        Assert.Equal("boom", ex.Message);
        Assert.NotNull(txn);
        Assert.True(txn!.RollbackCalled);
        Assert.Null(server.Transaction);
    }

    private class OpenFailureTransactionSqlServer : DBAClientX.SqlServer
    {
        public int DisposeCalls { get; private set; }

        protected override Task OpenConnectionAsync(SqlConnection connection, CancellationToken cancellationToken)
            => Task.FromException(new InvalidOperationException("boom"));

        protected override void DisposeConnection(SqlConnection connection)
            => DisposeCalls++;

        protected override ValueTask DisposeConnectionAsync(SqlConnection connection)
        {
            DisposeCalls++;
            return default;
        }
    }

    [Fact]
    public async Task BeginTransactionAsync_WhenOpenFails_DisposesConnectionAndResetsState()
    {
        using var server = new OpenFailureTransactionSqlServer();

        await Assert.ThrowsAsync<InvalidOperationException>(() => server.BeginTransactionAsync("s", "db", true));
        await Assert.ThrowsAsync<InvalidOperationException>(() => server.BeginTransactionAsync("s", "db", true));

        Assert.False(server.IsInTransaction);
        Assert.Equal(2, server.DisposeCalls);
    }

    private class BeginFailureTransactionSqlServer : DBAClientX.SqlServer
    {
        public int DisposeCalls { get; private set; }

        protected override Task OpenConnectionAsync(SqlConnection connection, CancellationToken cancellationToken)
            => Task.CompletedTask;

        protected override Task<SqlTransaction> BeginDbTransactionAsync(SqlConnection connection, IsolationLevel isolationLevel, CancellationToken cancellationToken)
            => Task.FromException<SqlTransaction>(new InvalidOperationException("boom"));

        protected override void DisposeConnection(SqlConnection connection)
            => DisposeCalls++;

        protected override ValueTask DisposeConnectionAsync(SqlConnection connection)
        {
            DisposeCalls++;
            return default;
        }
    }

    [Fact]
    public async Task BeginTransactionAsync_WhenBeginDbTransactionFails_DisposesConnectionAndResetsState()
    {
        using var server = new BeginFailureTransactionSqlServer();

        await Assert.ThrowsAsync<InvalidOperationException>(() => server.BeginTransactionAsync("s", "db", true));
        await Assert.ThrowsAsync<InvalidOperationException>(() => server.BeginTransactionAsync("s", "db", true));

        Assert.False(server.IsInTransaction);
        Assert.Equal(2, server.DisposeCalls);
    }

    private class AsyncDisposeTrackingSqlServer : DBAClientX.SqlServer
    {
        public int AsyncRollbackCalls { get; private set; }
        public int AsyncTransactionDisposals { get; private set; }
        public int AsyncConnectionDisposals { get; private set; }

        public void SeedActiveTransaction()
        {
            TransactionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(SqlTransaction)));
            TransactionConnectionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(SqlConnection)));
        }

        protected override ValueTask DisposeDbTransactionAsync(SqlTransaction transaction)
        {
            AsyncTransactionDisposals++;
            return default;
        }

        protected override Task TryRollbackDbTransactionOnDisposeAsync(SqlTransaction? transaction)
        {
            AsyncRollbackCalls++;
            return Task.CompletedTask;
        }

        protected override ValueTask DisposeConnectionAsync(SqlConnection connection)
        {
            AsyncConnectionDisposals++;
            return default;
        }

        protected override void TryRollbackDbTransactionOnDispose(SqlTransaction? transaction)
            => throw new InvalidOperationException("Synchronous rollback should not be used by DisposeAsync.");

        protected override void DisposeDbTransaction(SqlTransaction transaction)
            => throw new InvalidOperationException("Synchronous transaction disposal should not be used by DisposeAsync.");

        protected override void DisposeConnection(SqlConnection connection)
            => throw new InvalidOperationException("Synchronous connection disposal should not be used by DisposeAsync.");
    }

    [Fact]
    public async Task DisposeAsync_WithActiveTransaction_UsesAsyncCleanupAndClearsState()
    {
        var server = new AsyncDisposeTrackingSqlServer();
        server.SeedActiveTransaction();

        await server.DisposeAsync();
        await server.DisposeAsync();

        Assert.False(server.IsInTransaction);
        Assert.Equal(1, server.AsyncRollbackCalls);
        Assert.Equal(1, server.AsyncTransactionDisposals);
        Assert.Equal(1, server.AsyncConnectionDisposals);
    }
}
