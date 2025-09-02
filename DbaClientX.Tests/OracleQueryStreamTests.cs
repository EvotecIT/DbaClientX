using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime.CompilerServices;
using Oracle.ManagedDataAccess.Client;
using Xunit;

namespace DbaClientX.Tests;

public class OracleQueryStreamTests
{
    private class RowOracle : DBAClientX.Oracle
    {
        public override async IAsyncEnumerable<DataRow> QueryStreamAsync(string host, string serviceName, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, [EnumeratorCancellation] CancellationToken cancellationToken = default, IDictionary<string, OracleDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            var table = new DataTable();
            table.Columns.Add("id", typeof(int));
            for (int i = 1; i <= 2; i++)
            {
                var row = table.NewRow();
                row["id"] = i;
                table.Rows.Add(row);
            }
            for (int i = 0; i < table.Rows.Count; i++)
            {
                await Task.Yield();
                yield return table.Rows[i];
            }
        }
    }

    [Fact]
    public async Task QueryStreamAsync_EnumeratesRows()
    {
        using var oracle = new RowOracle();
        var results = new List<int>();
        await foreach (DataRow row in oracle.QueryStreamAsync("h", "svc", "u", "p", "q"))
        {
            results.Add((int)row["id"]);
        }
        Assert.Equal(new[] { 1, 2 }, results);
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
            await foreach (var _ in oracle.QueryStreamAsync("h", "svc", "u", "p", "q", cancellationToken: cts.Token))
            {
            }
        });
    }
}
