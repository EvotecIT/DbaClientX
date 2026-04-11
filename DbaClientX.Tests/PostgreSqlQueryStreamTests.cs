using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime.CompilerServices;
using Npgsql;
using NpgsqlTypes;
using Xunit;

namespace DbaClientX.Tests;

public class PostgreSqlQueryStreamTests
{
    private class CancelPg : DBAClientX.PostgreSql
    {
        public override async IAsyncEnumerable<DataRow> QueryStreamAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, [EnumeratorCancellation] CancellationToken cancellationToken = default, IDictionary<string, NpgsqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
            yield break;
        }
    }

    [Fact]
    public async Task QueryStreamAsync_CanBeCancelled()
    {
        using var pg = new CancelPg();
        using var cts = new CancellationTokenSource(100);
        await Assert.ThrowsAsync<TaskCanceledException>(async () =>
        {
            await foreach (var _ in pg.QueryStreamAsync("h", "d", "u", "p", "q", cancellationToken: cts.Token))
            {
            }
        });
    }

    private class OpenFailurePg : DBAClientX.PostgreSql
    {
        public int SyncDisposeCalls { get; private set; }
        public int AsyncDisposeCalls { get; private set; }

        protected override Task OpenConnectionAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
            => Task.FromException(new InvalidOperationException("boom"));

        protected override void DisposeConnection(NpgsqlConnection connection)
            => SyncDisposeCalls++;

        protected override ValueTask DisposeConnectionAsync(NpgsqlConnection connection)
        {
            AsyncDisposeCalls++;
            return default;
        }
    }

    [Fact]
    public async Task QueryStreamAsync_WhenOpenFails_UsesAsyncDispose()
    {
        using var pg = new OpenFailurePg();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await using var enumerator = pg.QueryStreamAsync("h", "d", "u", "p", "q").GetAsyncEnumerator();
            await enumerator.MoveNextAsync();
        });

        Assert.Equal("boom", ex.Message);
        Assert.Equal(0, pg.SyncDisposeCalls);
        Assert.Equal(1, pg.AsyncDisposeCalls);
    }

    [Fact]
    public void QueryStreamAsync_WithNullMapper_ThrowsBeforeOpeningConnection()
    {
        using var pg = new OpenFailurePg();
        const string connectionString = "Host=127.0.0.1;Port=1;Database=certwatch;Username=guest;Password=;SSL Mode=Disable";

        Assert.Throws<ArgumentNullException>(() =>
            pg.QueryStreamAsync<int>(connectionString, "SELECT 1", null!));

        Assert.Equal(0, pg.SyncDisposeCalls);
        Assert.Equal(0, pg.AsyncDisposeCalls);
    }
}
