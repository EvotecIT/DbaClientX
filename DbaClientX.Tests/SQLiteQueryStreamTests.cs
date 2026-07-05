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

    [Fact]
    public async Task QueryStreamWithConnectionStringAsync_PreservesMemoryMode()
    {
        var database = "dbaclientx-memory-" + Guid.NewGuid().ToString("N");
        using var sqlite = new DBAClientX.SQLite();
        var rows = 0;

        await foreach (var row in sqlite.QueryStreamWithConnectionStringAsync($"Data Source={database};Mode=Memory;Cache=Shared", "SELECT 1 AS Value"))
        {
            rows++;
            Assert.Equal(1L, row["Value"]);
        }

        Assert.Equal(1, rows);
        Assert.False(File.Exists(database));
    }
}
