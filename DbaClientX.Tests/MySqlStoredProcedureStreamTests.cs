using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using MySqlConnector;
using Xunit;

namespace DbaClientX.Tests;

public class MySqlStoredProcedureStreamTests
{
    private class DummyStoredProcMySql : DBAClientX.MySql
    {
        public List<MySqlParameter> Captured { get; } = new();

        public override IAsyncEnumerable<DataRow> ExecuteStoredProcedureStreamAsync(string host, string database, string username, string password, string procedure, IEnumerable<DbParameter>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default)
        {
            return Stream();

            async IAsyncEnumerable<DataRow> Stream()
            {
                using var command = new MySqlCommand(procedure);
                AddParameters(command, parameters);
                foreach (MySqlParameter p in command.Parameters)
                {
                    Captured.Add(p);
                }

                var table = new DataTable();
                table.Columns.Add("id", typeof(int));
                var row = table.NewRow();
                row["id"] = 1;
                table.Rows.Add(row);
                await Task.Yield();
                yield return row;
            }
        }
    }

    [Fact]
    public async Task ExecuteStoredProcedureStreamAsync_EnumeratesRows_AndBindsParameters()
    {
        using var mySql = new DummyStoredProcMySql();
        var param = new MySqlParameter("@id", MySqlDbType.Int32) { Value = 1 };
        var list = new List<int>();

        await foreach (DataRow row in mySql.ExecuteStoredProcedureStreamAsync("h", "d", "u", "p", "sp_test", new[] { param }))
        {
            list.Add((int)row["id"]);
        }

        Assert.Equal(new[] { 1 }, list);
        Assert.Contains(mySql.Captured, p => p.ParameterName == "@id" && p.Value is int v && v == 1);
    }
}

