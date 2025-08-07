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
        public Task<FakeSqlTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
        {
            BeginCalled = true;
            return Task.FromResult(new FakeSqlTransaction(this));
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
        public FakeSqlConnection? Connection { get; private set; }
        public FakeSqlTransaction? Transaction { get; private set; }

        public override async Task BeginTransactionAsync(string serverOrInstance, string database, bool integratedSecurity, CancellationToken cancellationToken = default, string? username = null, string? password = null)
        {
            Connection = new FakeSqlConnection();
            Transaction = await Connection.BeginTransactionAsync(cancellationToken);
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

        public override Task<object?> QueryAsync(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, SqlDbType>? parameterTypes = null, string? username = null, string? password = null)
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
}
