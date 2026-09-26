using System.Collections.ObjectModel;
using System.Data;
using System.Management.Automation;
using System.Management.Automation.Runspaces;
using System.Runtime.CompilerServices;
using DBAClientX;
using DBAClientX.PowerShell;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using NpgsqlTypes;
using Oracle.ManagedDataAccess.Client;

namespace DbaClientX.Tests;

/// <summary>
/// Covers R-33: <c>-Stream -ReturnType DataTable|DataSet</c> must return every streamed row, typed by the widest schema.
/// </summary>
[Collection(CmdletFactoryCollection.Name)]
public class StreamedDataTableCmdletTests
{
    [Theory]
    [InlineData(ReturnType.DataTable)]
    [InlineData(ReturnType.DataSet)]
    public void InvokeDbaXSQLite_StreamWithMixedTypes_ReturnsEveryRowWithWidestSchema(ReturnType returnType)
    {
        var path = Path.Combine(Path.GetTempPath(), "dbaclientx-stream-table-" + Guid.NewGuid().ToString("N") + ".db");
        try
        {
            using (var sqlite = new DBAClientX.SQLite())
            {
                sqlite.ExecuteNonQuery(path, "CREATE TABLE T (Id INTEGER PRIMARY KEY, Value); INSERT INTO T VALUES (1, NULL), (2, 10), (3, 'text'), (4, 2.5);");
            }

            var table = GetTable(Invoke(
                "Invoke-DbaXSQLite",
                typeof(CmdletInvokeDbaXSQLite),
                returnType,
                ("Database", path),
                ("Query", "SELECT Id, Value FROM T ORDER BY Id")));

            Assert.Equal(new object[] { 1L, 2L, 3L, 4L }, table.Rows.Cast<DataRow>().Select(row => row["Id"]).ToArray());
            Assert.Equal(new object[] { DBNull.Value, 10L, "text", 2.5d }, table.Rows.Cast<DataRow>().Select(row => row["Value"]).ToArray());
            Assert.Equal(typeof(long), table.Columns["Id"]!.DataType);
            Assert.Equal(typeof(object), table.Columns["Value"]!.DataType);
        }
        finally
        {
            Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
            File.Delete(path);
        }
    }

    [Theory]
    [InlineData(ReturnType.DataTable)]
    [InlineData(ReturnType.DataSet)]
    public void InvokeDbaXSQLite_StreamWithNoRows_KeepsEmptyResultContract(ReturnType returnType)
    {
        Collection<PSObject> results = Invoke(
            "Invoke-DbaXSQLite",
            typeof(CmdletInvokeDbaXSQLite),
            returnType,
            ("Database", ":memory:"),
            ("Query", "SELECT 1 AS Value WHERE 1 = 0"));

        if (returnType == ReturnType.DataTable)
        {
            Assert.Empty(results);
        }
        else
        {
            Assert.Empty(Assert.IsType<DataSet>(Assert.Single(results).BaseObject).Tables);
        }
    }

    [Theory]
    [InlineData(ReturnType.DataTable)]
    [InlineData(ReturnType.DataSet)]
    public void InvokeDbaXQuery_StreamWithDetachedRows_ReturnsEveryRow(ReturnType returnType)
    {
        CmdletIInvokeDbaXQuery.SqlServerFactory = () => new StreamSqlServer();
        try
        {
            AssertWidenedRows(GetTable(Invoke(
                "Invoke-DbaXQuery",
                typeof(CmdletIInvokeDbaXQuery),
                returnType,
                ("Server", "localhost"),
                ("Database", "tempdb"),
                ("Query", "SELECT 1"))));
        }
        finally
        {
            CmdletIInvokeDbaXQuery.SqlServerFactory = () => new DBAClientX.SqlServer();
        }
    }

    [Theory]
    [InlineData(ReturnType.DataTable)]
    [InlineData(ReturnType.DataSet)]
    public void InvokeDbaXMySql_StreamWithDetachedRows_ReturnsEveryRow(ReturnType returnType)
    {
        CmdletInvokeDbaXMySql.MySqlFactory = () => new StreamMySql();
        try
        {
            AssertWidenedRows(GetTable(InvokeWithCredentials("Invoke-DbaXMySql", typeof(CmdletInvokeDbaXMySql), returnType)));
        }
        finally
        {
            CmdletInvokeDbaXMySql.MySqlFactory = () => new DBAClientX.MySql();
        }
    }

    [Theory]
    [InlineData(ReturnType.DataTable)]
    [InlineData(ReturnType.DataSet)]
    public void InvokeDbaXPostgreSql_StreamWithDetachedRows_ReturnsEveryRow(ReturnType returnType)
    {
        CmdletInvokeDbaXPostgreSql.PostgreSqlFactory = () => new StreamPostgreSql();
        try
        {
            AssertWidenedRows(GetTable(InvokeWithCredentials("Invoke-DbaXPostgreSql", typeof(CmdletInvokeDbaXPostgreSql), returnType)));
        }
        finally
        {
            CmdletInvokeDbaXPostgreSql.PostgreSqlFactory = () => new DBAClientX.PostgreSql();
        }
    }

    [Theory]
    [InlineData(ReturnType.DataTable)]
    [InlineData(ReturnType.DataSet)]
    public void InvokeDbaXOracle_StreamWithDetachedRows_ReturnsEveryRow(ReturnType returnType)
    {
        CmdletInvokeDbaXOracle.OracleFactory = () => new StreamOracle();
        try
        {
            AssertWidenedRows(GetTable(InvokeWithCredentials("Invoke-DbaXOracle", typeof(CmdletInvokeDbaXOracle), returnType)));
        }
        finally
        {
            CmdletInvokeDbaXOracle.OracleFactory = () => new DBAClientX.Oracle();
        }
    }

    private static Collection<PSObject> InvokeWithCredentials(string name, Type cmdletType, ReturnType returnType)
        => Invoke(
            name,
            cmdletType,
            returnType,
            ("Server", "localhost"),
            ("Database", "app"),
            ("Username", "reader"),
            ("Password", "secret"),
            ("Query", "SELECT 1"));

    private static Collection<PSObject> Invoke(string name, Type cmdletType, ReturnType returnType, params (string Name, object Value)[] parameters)
    {
        var state = InitialSessionState.CreateDefault();
        state.Commands.Add(new SessionStateCmdletEntry(name, cmdletType, helpFileName: null));
        using var powerShell = PowerShell.Create(state);
        powerShell.AddCommand(name);
        foreach (var (parameterName, value) in parameters)
        {
            powerShell.AddParameter(parameterName, value);
        }

        powerShell
            .AddParameter("ReturnType", returnType)
            .AddParameter("Stream")
            .AddParameter("ErrorAction", ActionPreference.Stop);

        Collection<PSObject> results = powerShell.Invoke();
        Assert.False(powerShell.HadErrors);
        return results;
    }

    private static DataTable GetTable(Collection<PSObject> results)
    {
        var table = Assert.Single(results).BaseObject switch
        {
            DataTable dataTable => dataTable,
            DataSet set => Assert.Single(set.Tables.Cast<DataTable>()),
            var other => throw new Xunit.Sdk.XunitException($"Unexpected result type '{other.GetType().FullName}'."),
        };
        Assert.Equal("Table0", table.TableName);
        return table;
    }

    private static void AssertWidenedRows(DataTable table)
    {
        Assert.Equal(new object[] { 1, 2, 3 }, table.Rows.Cast<DataRow>().Select(row => row["Id"]).ToArray());
        Assert.Equal(new object[] { 10L, DBNull.Value, "text" }, table.Rows.Cast<DataRow>().Select(row => row["Value"]).ToArray());
        Assert.Equal(typeof(object), table.Columns["Value"]!.DataType);
    }

    /// <summary>
    /// Yields detached rows the way provider streaming does, switching to a wider source table for the last row.
    /// </summary>
    internal static async IAsyncEnumerable<DataRow> StreamDetachedRows([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var first = new DataTable();
        first.Columns.Add("Id", typeof(int));
        first.Columns.Add("Value", typeof(long));
        var widened = new DataTable();
        widened.Columns.Add("Id", typeof(int));
        widened.Columns.Add("Value", typeof(string));

        foreach (var (table, values) in new[]
        {
            (first, new object[] { 1, 10L }),
            (first, new object[] { 2, DBNull.Value }),
            (widened, new object[] { 3, "text" }),
        })
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.Yield();
            var row = table.NewRow();
            row.ItemArray = values;
            yield return row;
        }
    }

    private sealed class StreamSqlServer : DBAClientX.SqlServer
    {
        public override IAsyncEnumerable<DataRow> QueryStreamAsync(string connectionString, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, SqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
            => StreamDetachedRows(cancellationToken);
    }

    private sealed class StreamMySql : DBAClientX.MySql
    {
        public override IAsyncEnumerable<DataRow> QueryStreamAsync(string connectionString, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, MySqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
            => StreamDetachedRows(cancellationToken);
    }

    private sealed class StreamPostgreSql : DBAClientX.PostgreSql
    {
        public override IAsyncEnumerable<DataRow> QueryStreamAsync(string connectionString, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, NpgsqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
            => StreamDetachedRows(cancellationToken);
    }

    private sealed class StreamOracle : DBAClientX.Oracle
    {
        public override IAsyncEnumerable<DataRow> QueryStreamAsync(string connectionString, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, OracleDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
            => StreamDetachedRows(cancellationToken);
    }
}
