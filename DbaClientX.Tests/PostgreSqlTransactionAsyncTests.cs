using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NpgsqlTypes;
using Xunit;

namespace DbaClientX.Tests;

public class PostgreSqlTransactionAsyncTests
{
    private class FakeNpgsqlConnection
    {
        public bool BeginCalled { get; private set; }
        public Task<FakeNpgsqlTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
        {
            BeginCalled = true;
            return Task.FromResult(new FakeNpgsqlTransaction(this));
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

    private class TestPostgreSql : DBAClientX.PostgreSql
    {
        public FakeNpgsqlConnection? Connection { get; private set; }
        public FakeNpgsqlTransaction? Transaction { get; private set; }

        public override async Task BeginTransactionAsync(string host, string database, string username, string password, CancellationToken cancellationToken = default)
        {
            Connection = new FakeNpgsqlConnection();
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

        public override Task<object?> QueryAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, NpgsqlDbType>? parameterTypes = null)
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
        using var pg = new TestPostgreSql();
        await pg.BeginTransactionAsync("h", "d", "u", "p").ConfigureAwait(false);
        Assert.NotNull(pg.Connection);
        Assert.True(pg.Connection!.BeginCalled);
        Assert.NotNull(pg.Transaction);
    }

    [Fact]
    public async Task CommitAsync_CallsCommitOnTransaction()
    {
        using var pg = new TestPostgreSql();
        await pg.BeginTransactionAsync("h", "d", "u", "p").ConfigureAwait(false);
        var txn = pg.Transaction!;
        await pg.CommitAsync().ConfigureAwait(false);
        Assert.True(txn.CommitCalled);
        Assert.Null(pg.Transaction);
    }

    [Fact]
    public async Task RollbackAsync_CallsRollbackOnTransaction()
    {
        using var pg = new TestPostgreSql();
        await pg.BeginTransactionAsync("h", "d", "u", "p").ConfigureAwait(false);
        var txn = pg.Transaction!;
        await pg.RollbackAsync().ConfigureAwait(false);
        Assert.True(txn.RollbackCalled);
        Assert.Null(pg.Transaction);
    }
}
