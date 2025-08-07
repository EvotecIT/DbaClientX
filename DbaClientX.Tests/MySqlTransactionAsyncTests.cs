using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MySqlConnector;
using Xunit;

namespace DbaClientX.Tests;

public class MySqlTransactionAsyncTests
{
    private class FakeMySqlConnection
    {
        public bool BeginCalled { get; private set; }
        public Task<FakeMySqlTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
        {
            BeginCalled = true;
            return Task.FromResult(new FakeMySqlTransaction(this));
        }
    }

    private class FakeMySqlTransaction
    {
        private readonly FakeMySqlConnection _connection;
        public bool CommitCalled { get; private set; }
        public bool RollbackCalled { get; private set; }

        public FakeMySqlTransaction(FakeMySqlConnection connection)
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

    private class TestMySql : DBAClientX.MySql
    {
        public FakeMySqlConnection? Connection { get; private set; }
        public FakeMySqlTransaction? Transaction { get; private set; }

        public override async Task BeginTransactionAsync(string host, string database, string username, string password, CancellationToken cancellationToken = default)
        {
            Connection = new FakeMySqlConnection();
            Transaction = await Connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
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

        public override Task<object?> QueryAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, MySqlDbType>? parameterTypes = null)
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
        using var mySql = new TestMySql();
        await mySql.BeginTransactionAsync("h", "d", "u", "p").ConfigureAwait(false);
        Assert.NotNull(mySql.Connection);
        Assert.True(mySql.Connection!.BeginCalled);
        Assert.NotNull(mySql.Transaction);
    }

    [Fact]
    public async Task CommitAsync_CallsCommitOnTransaction()
    {
        using var mySql = new TestMySql();
        await mySql.BeginTransactionAsync("h", "d", "u", "p").ConfigureAwait(false);
        var txn = mySql.Transaction!;
        await mySql.CommitAsync().ConfigureAwait(false);
        Assert.True(txn.CommitCalled);
        Assert.Null(mySql.Transaction);
    }

    [Fact]
    public async Task RollbackAsync_CallsRollbackOnTransaction()
    {
        using var mySql = new TestMySql();
        await mySql.BeginTransactionAsync("h", "d", "u", "p").ConfigureAwait(false);
        var txn = mySql.Transaction!;
        await mySql.RollbackAsync().ConfigureAwait(false);
        Assert.True(txn.RollbackCalled);
        Assert.Null(mySql.Transaction);
    }
}
