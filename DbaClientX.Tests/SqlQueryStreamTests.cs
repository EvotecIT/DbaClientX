using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime.CompilerServices;
using Xunit;

namespace DbaClientX.Tests;

public class SqlQueryStreamTests
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

        public override async IAsyncEnumerable<DataRow> SqlQueryStreamAsync(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, [EnumeratorCancellation] CancellationToken cancellationToken = default, IDictionary<string, SqlDbType>? parameterTypes = null, string? username = null, string? password = null)
        {
            foreach (var row in _rows)
            {
                await Task.Yield();
                yield return row;
            }
        }
    }

    [Fact]
    public async Task SqlQueryStreamAsync_EnumeratesRows()
    {
        var server = new DummySqlServer();
        var list = new List<int>();

        await foreach (DataRow row in server.SqlQueryStreamAsync("s", "d", true, "q"))
        {
            list.Add((int)row["id"]);
        }

        Assert.Equal(new[] { 1, 2 }, list);
    }
}
