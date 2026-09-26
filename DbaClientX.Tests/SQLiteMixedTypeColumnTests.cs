using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.IO;
using System.Linq;
using System.Management.Automation;
using System.Management.Automation.Runspaces;
using System.Threading.Tasks;
using DBAClientX;
using DBAClientX.PowerShell;
using Xunit;

namespace DbaClientX.Tests;

public class SQLiteMixedTypeColumnTests
{
    private const string CreateTableSql =
        "CREATE TABLE T (Id TEXT PRIMARY KEY, Name TEXT NULL, FirstSeenUtcMs INTEGER NOT NULL DEFAULT 0, Label TEXT DEFAULT 'x', OccurrenceCount INTEGER NOT NULL DEFAULT 1);";

    private const string PragmaTableInfoSql = "PRAGMA table_info([T]);";

    private const string MixedUnionSql = "SELECT 1 AS Value UNION ALL SELECT 'a' UNION ALL SELECT NULL UNION ALL SELECT 2.5 UNION ALL SELECT x'0102';";

    [Theory]
    [InlineData(ReturnType.DataTable)]
    [InlineData(ReturnType.PSObject)]
    [InlineData(ReturnType.DataSet)]
    public void Query_PragmaTableInfoWithMixedDefaults_ReturnsEveryColumn(ReturnType returnType)
    {
        var path = CreateDatabase();
        try
        {
            using var sqlite = new DBAClientX.SQLite { ReturnType = returnType };

            var table = GetTable(sqlite.Query(path, PragmaTableInfoSql));

            AssertPragmaTableInfo(table);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Theory]
    [InlineData(ReturnType.DataTable)]
    [InlineData(ReturnType.DataSet)]
    public async Task QueryAsync_PragmaTableInfoWithMixedDefaults_ReturnsEveryColumn(ReturnType returnType)
    {
        var path = CreateDatabase();
        try
        {
            using var sqlite = new DBAClientX.SQLite { ReturnType = returnType };

            var table = GetTable(await sqlite.QueryAsync(path, PragmaTableInfoSql));

            AssertPragmaTableInfo(table);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public void Query_UnionAllWithMixedStorageClasses_MaterializesObjectColumn()
    {
        using var sqlite = new DBAClientX.SQLite { ReturnType = ReturnType.DataTable };

        var table = Assert.IsType<DataTable>(sqlite.Query(":memory:", MixedUnionSql));

        AssertMixedUnion(table.Rows.Cast<DataRow>().ToList());
        Assert.Equal(typeof(object), table.Columns["Value"]!.DataType);
    }

    [Fact]
    public async Task QueryAsync_UnionAllWithMixedStorageClasses_MaterializesObjectColumn()
    {
        using var sqlite = new DBAClientX.SQLite { ReturnType = ReturnType.DataSet };

        var dataSet = Assert.IsType<DataSet>(await sqlite.QueryAsync(":memory:", MixedUnionSql));

        var table = Assert.Single(dataSet.Tables.Cast<DataTable>());
        AssertMixedUnion(table.Rows.Cast<DataRow>().ToList());
        Assert.Equal(typeof(object), table.Columns["Value"]!.DataType);
    }

    [Fact]
    public void Query_NullBeforeTextValues_UsesObservedValueType()
    {
        using var sqlite = new DBAClientX.SQLite { ReturnType = ReturnType.DataTable };

        var table = Assert.IsType<DataTable>(sqlite.Query(":memory:", "SELECT NULL AS Value UNION ALL SELECT 'a' UNION ALL SELECT 'b';"));

        Assert.Equal(typeof(string), table.Columns["Value"]!.DataType);
        Assert.Equal(new object[] { DBNull.Value, "a", "b" }, table.Rows.Cast<DataRow>().Select(row => row["Value"]).ToArray());
    }

    [Fact]
    public void Query_HomogeneousColumns_KeepsProviderFieldTypes()
    {
        using var sqlite = new DBAClientX.SQLite { ReturnType = ReturnType.DataTable };

        var table = Assert.IsType<DataTable>(sqlite.Query(
            ":memory:",
            "SELECT 1 AS Number, 'a' AS Text, 1.5 AS Real, NULL AS Missing UNION ALL SELECT 2, 'b', NULL, NULL;"));

        Assert.Equal(typeof(long), table.Columns["Number"]!.DataType);
        Assert.Equal(typeof(string), table.Columns["Text"]!.DataType);
        Assert.Equal(typeof(double), table.Columns["Real"]!.DataType);
        Assert.Equal(new[] { "Number", "Text", "Real", "Missing" }, table.Columns.Cast<DataColumn>().Select(column => column.ColumnName).ToArray());
        Assert.Equal(2L, table.Rows[1]["Number"]);
        Assert.Equal("b", table.Rows[1]["Text"]);
        Assert.Equal(DBNull.Value, table.Rows[1]["Real"]);
    }

    [Fact]
    public void Query_MixedColumnBesideTypedColumns_PreservesOrdinalsAndValues()
    {
        using var sqlite = new DBAClientX.SQLite { ReturnType = ReturnType.DataTable };

        var table = Assert.IsType<DataTable>(sqlite.Query(
            ":memory:",
            "SELECT 1 AS Id, 10 AS Mixed, 'x' AS Tail UNION ALL SELECT 2, 'ten', 'y' UNION ALL SELECT 3, NULL, 'z';"));

        Assert.Equal(new[] { "Id", "Mixed", "Tail" }, table.Columns.Cast<DataColumn>().Select(column => column.ColumnName).ToArray());
        Assert.Equal(typeof(long), table.Columns["Id"]!.DataType);
        Assert.Equal(typeof(object), table.Columns["Mixed"]!.DataType);
        Assert.Equal(typeof(string), table.Columns["Tail"]!.DataType);
        Assert.Equal(new object[] { 10L, "ten", DBNull.Value }, table.Rows.Cast<DataRow>().Select(row => row["Mixed"]).ToArray());
        Assert.Equal(new object[] { "x", "y", "z" }, table.Rows.Cast<DataRow>().Select(row => row["Tail"]).ToArray());
    }

    [Theory]
    [InlineData("ASC")]
    [InlineData("DESC")]
    public void Query_NumericAffinityWithIntegerAndReal_MaterializesDoubleColumn(string order)
    {
        var path = CreateDatabase();
        try
        {
            using var sqlite = new DBAClientX.SQLite { ReturnType = ReturnType.DataTable };
            sqlite.ExecuteNonQuery(path, "CREATE TABLE P (Id INTEGER, Price DECIMAL(10,2)); INSERT INTO P VALUES (1, 1.5), (2, 2), (3, NULL);");

            var table = Assert.IsType<DataTable>(sqlite.Query(path, $"SELECT Price FROM P ORDER BY Id {order};"));

            Assert.Equal(typeof(double), table.Columns["Price"]!.DataType);
            var prices = table.Rows.Cast<DataRow>().Select(row => row["Price"]).ToList();
            Assert.Contains(1.5d, prices);
            Assert.Contains(2d, prices);
            Assert.Contains(DBNull.Value, prices);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public void Query_IntegerBeyondDoublePrecisionWithReal_MaterializesObjectColumn()
    {
        using var sqlite = new DBAClientX.SQLite { ReturnType = ReturnType.DataTable };

        var table = Assert.IsType<DataTable>(sqlite.Query(":memory:", "SELECT 9007199254740993 AS Value UNION ALL SELECT 1.5;"));

        Assert.Equal(typeof(object), table.Columns["Value"]!.DataType);
        Assert.Equal(9007199254740993L, table.Rows[0]["Value"]);
        Assert.Equal(1.5d, table.Rows[1]["Value"]);
    }

    [Fact]
    public void ReadDataTable_AdaptationDisabled_KeepsFirstRowFieldTypeContract()
    {
        using var client = new ReadDataTableClient();
        using var connection = new Microsoft.Data.Sqlite.SqliteConnection("Data Source=:memory:");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 AS Value UNION ALL SELECT 'a';";

        using (var reader = command.ExecuteReader())
        {
            Assert.False(client.AdaptsByDefault);
            Assert.Throws<ArgumentException>(() => ReadDataTableClient.Read(reader, adapt: false));
        }

        using (var reader = command.ExecuteReader())
        {
            var table = ReadDataTableClient.Read(reader, adapt: true);
            Assert.Equal(typeof(object), table.Columns["Value"]!.DataType);
            Assert.Equal(new object[] { 1L, "a" }, table.Rows.Cast<DataRow>().Select(row => row["Value"]).ToArray());
        }
    }

    [Fact]
    public void Query_DataRowReturnTypeWithMixedStorageClasses_ReturnsFirstRow()
    {
        using var sqlite = new DBAClientX.SQLite { ReturnType = ReturnType.DataRow };

        var row = Assert.IsType<DataRow>(sqlite.Query(":memory:", MixedUnionSql));

        Assert.Equal(1L, row["Value"]);
    }

    [Fact]
    public async Task QueryStreamAsync_UnionAllWithMixedStorageClasses_YieldsEveryValue()
    {
        using var sqlite = new DBAClientX.SQLite();
        var rows = new List<DataRow>();

        await foreach (var row in sqlite.QueryStreamAsync(":memory:", MixedUnionSql))
        {
            rows.Add(row);
        }

        AssertMixedUnion(rows);
    }

    [Fact]
    public async Task QueryStreamAsync_DuplicateColumnNames_UsesUniqueNamesLikeBufferedQueries()
    {
        using var sqlite = new DBAClientX.SQLite();
        var rows = new List<DataRow>();

        await foreach (var row in sqlite.QueryStreamAsync(":memory:", "SELECT 1 AS a, 2 AS a"))
        {
            rows.Add(row);
        }

        var single = Assert.Single(rows);
        Assert.Equal(new[] { "a", "a1" }, single.Table.Columns.Cast<DataColumn>().Select(column => column.ColumnName).ToArray());
        Assert.Equal(new object[] { 1L, 2L }, single.ItemArray);
    }

    [Fact]
    public async Task Query_DataRowReturnTypeWithDuplicateColumnNames_UsesUniqueNames()
    {
        using var sqlite = new DBAClientX.SQLite { ReturnType = ReturnType.DataRow };

        var syncRow = Assert.IsType<DataRow>(sqlite.Query(":memory:", "SELECT 1 AS a, 2 AS A"));
        var asyncRow = Assert.IsType<DataRow>(await sqlite.QueryAsync(":memory:", "SELECT 1 AS a, 2 AS A"));

        foreach (var row in new[] { syncRow, asyncRow })
        {
            Assert.Equal(new[] { "a", "A1" }, row.Table.Columns.Cast<DataColumn>().Select(column => column.ColumnName).ToArray());
            Assert.Equal(new object[] { 1L, 2L }, row.ItemArray);
        }
    }

    [Fact]
    public async Task QueryStreamAsync_PragmaTableInfoWithMixedDefaults_YieldsEveryColumn()
    {
        var path = CreateDatabase();
        try
        {
            using var sqlite = new DBAClientX.SQLite();
            var rows = new List<DataRow>();

            await foreach (var row in sqlite.QueryStreamAsync(path, PragmaTableInfoSql))
            {
                rows.Add(row);
            }

            AssertPragmaTableInfoRows(rows);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task QueryAsListAsync_PragmaTableInfoWithMixedDefaults_MapsEveryColumn()
    {
        var path = CreateDatabase();
        try
        {
            using var sqlite = new DBAClientX.SQLite();

            var defaults = await sqlite.QueryAsListAsync(path, PragmaTableInfoSql, record => record.IsDBNull(4) ? null : record.GetValue(4));

            Assert.Equal(new object?[] { null, null, "0", "'x'", "1" }, defaults.ToArray());
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Theory]
    [InlineData(ReturnType.PSObject, false)]
    [InlineData(ReturnType.DataTable, false)]
    [InlineData(ReturnType.DataSet, false)]
    [InlineData(ReturnType.DataRow, false)]
    [InlineData(ReturnType.PSObject, true)]
    [InlineData(ReturnType.DataTable, true)]
    [InlineData(ReturnType.DataSet, true)]
    [InlineData(ReturnType.DataRow, true)]
    public void InvokeDbaXSQLite_PragmaTableInfoWithMixedDefaults_ReturnsEveryColumn(ReturnType returnType, bool stream)
    {
        var path = CreateDatabase();
        try
        {
            var state = InitialSessionState.CreateDefault();
            state.Commands.Add(new SessionStateCmdletEntry("Invoke-DbaXSQLite", typeof(CmdletInvokeDbaXSQLite), helpFileName: null));
            using var powerShell = PowerShell.Create(state);
            powerShell
                .AddCommand("Invoke-DbaXSQLite")
                .AddParameter("Database", path)
                .AddParameter("Query", PragmaTableInfoSql)
                .AddParameter("ReturnType", returnType)
                .AddParameter("ErrorAction", ActionPreference.Stop);
            if (stream)
            {
                powerShell.AddParameter("Stream");
            }

            Collection<PSObject> results = powerShell.Invoke();

            Assert.False(powerShell.HadErrors);
            if (returnType == ReturnType.DataRow && !stream)
            {
                // Buffered DataRow mode returns only the first row by design.
                var firstRow = Assert.IsType<DataRow>(Assert.Single(results).BaseObject);
                Assert.Equal("Id", firstRow["name"]);
                Assert.Equal(DBNull.Value, firstRow["dflt_value"]);
                return;
            }

            var defaults = returnType switch
            {
                ReturnType.PSObject => results.Select(result => result.Properties["dflt_value"].Value).ToArray(),
                ReturnType.DataRow => results.Select(result => ((DataRow)result.BaseObject)["dflt_value"]).ToArray(),
                ReturnType.DataTable => Assert.IsType<DataTable>(Assert.Single(results).BaseObject).Rows.Cast<DataRow>().Select(row => row["dflt_value"]).ToArray(),
                _ => Assert.IsType<DataSet>(Assert.Single(results).BaseObject).Tables[0].Rows.Cast<DataRow>().Select(row => row["dflt_value"]).ToArray(),
            };
            Assert.Equal(5, defaults.Length);
            Assert.Equal("0", defaults[2]);
            Assert.Equal("'x'", defaults[3]);
            Assert.Equal("1", defaults[4]);
        }
        finally
        {
            Cleanup(path);
        }
    }

    private sealed class ReadDataTableClient : DatabaseClientBase
    {
        public bool AdaptsByDefault => AdaptResultColumnTypesToValues;

        public static DataTable Read(System.Data.Common.DbDataReader reader, bool adapt) => ReadDataTable(reader, "Table0", adapt);
    }

    private static DataTable GetTable(object? result)
        => result switch
        {
            DataTable table => table,
            DataSet dataSet => Assert.Single(dataSet.Tables.Cast<DataTable>()),
            _ => throw new Xunit.Sdk.XunitException($"Unexpected result type '{result?.GetType().FullName ?? "null"}'."),
        };

    private static void AssertPragmaTableInfo(DataTable table)
    {
        Assert.Equal(new[] { "cid", "name", "type", "notnull", "dflt_value", "pk" }, table.Columns.Cast<DataColumn>().Select(column => column.ColumnName).ToArray());
        AssertPragmaTableInfoRows(table.Rows.Cast<DataRow>().ToList());
    }

    private static void AssertPragmaTableInfoRows(IReadOnlyList<DataRow> rows)
    {
        Assert.Equal(new object[] { "Id", "Name", "FirstSeenUtcMs", "Label", "OccurrenceCount" }, rows.Select(row => row["name"]).ToArray());
        Assert.Equal(new object[] { DBNull.Value, DBNull.Value, "0", "'x'", "1" }, rows.Select(row => row["dflt_value"]).ToArray());
        Assert.Equal(new object[] { 0L, 0L, 1L, 0L, 1L }, rows.Select(row => row["notnull"]).ToArray());
        Assert.Equal(new object[] { 1L, 0L, 0L, 0L, 0L }, rows.Select(row => row["pk"]).ToArray());
    }

    private static void AssertMixedUnion(IReadOnlyList<DataRow> rows)
    {
        Assert.Equal(5, rows.Count);
        Assert.Equal(1L, rows[0]["Value"]);
        Assert.Equal("a", rows[1]["Value"]);
        Assert.Equal(DBNull.Value, rows[2]["Value"]);
        Assert.Equal(2.5d, rows[3]["Value"]);
        Assert.Equal(new byte[] { 1, 2 }, rows[4]["Value"]);
    }

    private static string CreateDatabase()
    {
        var path = Path.Combine(Path.GetTempPath(), "dbaclientx-mixed-types-" + Guid.NewGuid().ToString("N") + ".db");
        using var sqlite = new DBAClientX.SQLite();
        sqlite.ExecuteNonQuery(path, CreateTableSql);
        return path;
    }

    private static void Cleanup(string path)
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        foreach (var candidate in new[] { path, path + "-wal", path + "-shm" })
        {
            try
            {
                if (File.Exists(candidate))
                {
                    File.Delete(candidate);
                }
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }
    }
}
