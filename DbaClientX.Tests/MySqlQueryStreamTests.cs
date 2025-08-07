using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime.CompilerServices;
using MySqlConnector;
using Xunit;

namespace DbaClientX.Tests;

public class MySqlQueryStreamTests
{
    private class DummyMySql : DBAClientX.MySql
    {
        private readonly List<DataRow> _rows;

        public DummyMySql()
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

        public override async IAsyncEnumerable<DataRow> QueryStreamAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, [EnumeratorCancellation] CancellationToken cancellationToken = default, IDictionary<string, MySqlDbType>? parameterTypes = null)
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
        using var mySql = new DummyMySql();
        var list = new List<int>();

        await foreach (DataRow row in mySql.QueryStreamAsync("h", "d", "u", "p", "q").ConfigureAwait(false))
        {
            list.Add((int)row["id"]);
        }

        Assert.Equal(new[] { 1, 2 }, list);
    }
}
