using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime.CompilerServices;
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
}
