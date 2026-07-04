using System.Data;
using System.Collections.ObjectModel;
using System.Management.Automation;
using System.Management.Automation.Runspaces;
using DBAClientX;
using DBAClientX.PowerShell;
using Microsoft.Data.SqlClient;

namespace DbaClientX.Tests;

public class InvokeDbaXQueryCmdletTests
{
    [Fact]
    public void DataTableReturnType_EmitsSingleDataTable()
    {
        using var table = CreateTable();
        CmdletIInvokeDbaXQuery.SqlServerFactory = () => new DataTableSqlServer(table);

        try
        {
            var results = InvokeQuery(ReturnType.DataTable);

            var result = Assert.Single(results);
            var resultTable = Assert.IsType<DataTable>(result.BaseObject);
            Assert.Equal(2, resultTable.Rows.Count);
        }
        finally
        {
            CmdletIInvokeDbaXQuery.SqlServerFactory = () => new SqlServer();
        }
    }

    [Fact]
    public void DataRowReturnType_EmitsEachDataRow()
    {
        using var table = CreateTable();
        CmdletIInvokeDbaXQuery.SqlServerFactory = () => new DataTableSqlServer(table);

        try
        {
            var results = InvokeQuery(ReturnType.DataRow);

            Assert.Equal(2, results.Count);
            Assert.All(results, result => Assert.IsType<DataRow>(result.BaseObject));
        }
        finally
        {
            CmdletIInvokeDbaXQuery.SqlServerFactory = () => new SqlServer();
        }
    }

    private sealed class DataTableSqlServer : SqlServer
    {
        private readonly DataTable _table;

        public DataTableSqlServer(DataTable table)
        {
            _table = table;
        }

        public override object? Query(
            string connectionString,
            string query,
            IDictionary<string, object?>? parameters = null,
            bool useTransaction = false,
            IDictionary<string, SqlDbType>? parameterTypes = null,
            IDictionary<string, ParameterDirection>? parameterDirections = null)
            => ReturnType == ReturnType.DataRow ? _table.Rows : _table;
    }

    private static DataTable CreateTable()
    {
        var table = new DataTable("Rows");
        table.Columns.Add("Id", typeof(int));
        table.Rows.Add(1);
        table.Rows.Add(2);
        return table;
    }

    private static Collection<PSObject> InvokeQuery(ReturnType returnType)
    {
        var state = InitialSessionState.CreateDefault();
        state.Commands.Add(new SessionStateCmdletEntry("Invoke-DbaXQuery", typeof(CmdletIInvokeDbaXQuery), helpFileName: null));

        using var powerShell = PowerShell.Create(state);
        powerShell
            .AddCommand("Invoke-DbaXQuery")
            .AddParameter("Server", "localhost")
            .AddParameter("Database", "tempdb")
            .AddParameter("Query", "SELECT 1")
            .AddParameter("ReturnType", returnType);

        return powerShell.Invoke();
    }
}
