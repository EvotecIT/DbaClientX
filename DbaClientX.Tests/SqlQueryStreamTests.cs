using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime.CompilerServices;
using Microsoft.Data.SqlClient;
using Xunit;

namespace DbaClientX.Tests;

public class QueryStreamTests
{
    private class DummySqlServer : DBAClientX.SqlServer
    {
        private readonly List<DataRow> _rows;

        public DummySqlServer()
        {
            var table = new DataTable();
            table.Columns.Add("id", typeof(int));
            table.Columns.Add("name", typeof(string));
            var r1 = table.NewRow();
            r1["id"] = 1;
            r1["name"] = "one";
            table.Rows.Add(r1);
            var r2 = table.NewRow();
            r2["id"] = 2;
            r2["name"] = "two";
            table.Rows.Add(r2);
            _rows = table.Rows.Cast<DataRow>().ToList();
        }

        public override async IAsyncEnumerable<DataRow> QueryStreamAsync(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, [EnumeratorCancellation] CancellationToken cancellationToken = default, IDictionary<string, SqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null, string? username = null, string? password = null)
        {
            foreach (var row in _rows)
            {
                await Task.Yield();
                yield return row;
            }
        }
    }

    [Fact]
    public async Task QueryStreamAsync_EnumeratesRows()
    {
        using var server = new DummySqlServer();
        var list = new List<int>();

        await foreach (DataRow row in server.QueryStreamAsync("s", "d", true, "q"))
        {
            list.Add((int)row["id"]);
        }

        Assert.Equal(new[] { 1, 2 }, list);
    }

    private class CancelSqlServer : DBAClientX.SqlServer
    {
        public override async IAsyncEnumerable<DataRow> QueryStreamAsync(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, [EnumeratorCancellation] CancellationToken cancellationToken = default, IDictionary<string, SqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null, string? username = null, string? password = null)
        {
            await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
            yield break;
        }
    }

    [Fact]
    public async Task QueryStreamAsync_CanBeCancelled()
    {
        using var server = new CancelSqlServer();
        using var cts = new CancellationTokenSource(100);
        await Assert.ThrowsAsync<TaskCanceledException>(async () =>
        {
            await foreach (var _ in server.QueryStreamAsync("s", "d", true, "q", cancellationToken: cts.Token))
            {
            }
        });
    }

    private class OpenFailureSqlServer : DBAClientX.SqlServer
    {
        public int SyncDisposeCalls { get; private set; }
        public int AsyncDisposeCalls { get; private set; }

        protected override Task OpenConnectionAsync(SqlConnection connection, CancellationToken cancellationToken)
            => Task.FromException(new InvalidOperationException("boom"));

        protected override void DisposeConnection(SqlConnection connection)
            => SyncDisposeCalls++;

        protected override ValueTask DisposeConnectionAsync(SqlConnection connection)
        {
            AsyncDisposeCalls++;
            return default;
        }
    }

    [Fact]
    public async Task QueryStreamAsync_WhenOpenFails_UsesAsyncDispose()
    {
        using var server = new OpenFailureSqlServer();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await using var enumerator = server.QueryStreamAsync("s", "d", true, "q").GetAsyncEnumerator();
            await enumerator.MoveNextAsync();
        });

        Assert.Equal("boom", ex.Message);
        Assert.Equal(0, server.SyncDisposeCalls);
        Assert.Equal(1, server.AsyncDisposeCalls);
    }
}
