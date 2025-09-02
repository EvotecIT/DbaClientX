using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime.CompilerServices;
using Oracle.ManagedDataAccess.Client;
using Xunit;

namespace DbaClientX.Tests;

public class OracleQueryStreamTests
{
    private class DummyOracle : DBAClientX.Oracle
    {
        private readonly List<DataRow> _rows;

        public DummyOracle()
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

        public override async IAsyncEnumerable<DataRow> QueryStreamAsync(string host, string serviceName, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, [EnumeratorCancellation] CancellationToken cancellationToken = default, IDictionary<string, OracleDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
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
        using var oracle = new DummyOracle();
        var list = new List<int>();

        await foreach (DataRow row in oracle.QueryStreamAsync("h", "s", "u", "p", "q"))
        {
            list.Add((int)row["id"]);
        }

        Assert.Equal(new[] { 1, 2 }, list);
    }

    private class CancelOracle : DBAClientX.Oracle
    {
        public override async IAsyncEnumerable<DataRow> QueryStreamAsync(string host, string serviceName, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, [EnumeratorCancellation] CancellationToken cancellationToken = default, IDictionary<string, OracleDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
            yield break;
        }
    }

    [Fact]
    public async Task QueryStreamAsync_CanBeCancelled()
    {
        using var oracle = new CancelOracle();
        using var cts = new CancellationTokenSource(100);
        await Assert.ThrowsAsync<TaskCanceledException>(async () =>
        {
            await foreach (var _ in oracle.QueryStreamAsync("h", "s", "u", "p", "q", cancellationToken: cts.Token))
            {
            }
        });
    }
}
