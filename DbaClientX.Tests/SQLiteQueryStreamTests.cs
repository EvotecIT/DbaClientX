using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;
using Xunit;

namespace DbaClientX.Tests;

public class SQLiteQueryStreamTests
{
    private class CancelSqlite : DBAClientX.SQLite
    {
        public override async IAsyncEnumerable<DataRow> QueryStreamAsync(string database, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, [EnumeratorCancellation] CancellationToken cancellationToken = default, IDictionary<string, SqliteType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
            yield break;
        }
    }

    [Fact]
    public async Task QueryStreamAsync_CanBeCancelled()
    {
        using var sqlite = new CancelSqlite();
        using var cts = new CancellationTokenSource(100);
        await Assert.ThrowsAsync<TaskCanceledException>(async () =>
        {
            await foreach (var _ in sqlite.QueryStreamAsync(":memory:", "q", cancellationToken: cts.Token))
            {
            }
        });
    }
}
