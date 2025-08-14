using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace DbaClientX.Tests;

public class SqlServerTransactionAsyncTests
{
    private class FakeSqlConnection
    {
        public bool BeginCalled { get; private set; }
        public async Task<FakeSqlTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
        {
            BeginCalled = true;
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
            var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

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
            await Transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            Transaction = null;
        }

        public override async Task RollbackAsync(CancellationToken cancellationToken = default)
        {
            if (Transaction == null)
            {
                throw new DBAClientX.DbaTransactionException("No active transaction.");
            }
            await Transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
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
        await server.BeginTransactionAsync("s", "db", true).ConfigureAwait(false);
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

        await Assert.ThrowsAsync<DBAClientX.DbaTransactionException>(() => Task.WhenAll(tasks)).ConfigureAwait(false);
        Assert.NotNull(server.Transaction);
    }

    [Fact]
    public async Task CommitAsync_CallsCommitOnTransaction()
    {
        using var server = new TestSqlServer();
        await server.BeginTransactionAsync("s", "db", true).ConfigureAwait(false);
        var txn = server.Transaction!;
        await server.CommitAsync().ConfigureAwait(false);
        Assert.True(txn.CommitCalled);
        Assert.Null(server.Transaction);
    }

    [Fact]
    public async Task RollbackAsync_CallsRollbackOnTransaction()
    {
        using var server = new TestSqlServer();
        await server.BeginTransactionAsync("s", "db", true).ConfigureAwait(false);
        var txn = server.Transaction!;
        await server.RollbackAsync().ConfigureAwait(false);
        Assert.True(txn.RollbackCalled);
        Assert.Null(server.Transaction);
    }
}
