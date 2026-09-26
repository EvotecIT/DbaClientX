using System.Data;
using System.Management.Automation;
using DBAClientX;
using DBAClientX.PowerShell;

namespace DbaClientX.Tests;

public class DbaXResultWriterTests
{
    [Fact]
    public void WriteRows_DataSetReturnType_WritesDataSet()
    {
        using var table = new DataTable("Rows");
        table.Columns.Add("Name", typeof(string));
        table.Rows.Add("Ada");

        var output = new List<object?>();

        DbaXResultWriter.WriteRows(
            table.Rows.Cast<DataRow>(),
            ReturnType.DataSet,
            (value, _) => output.Add(value));

        using var dataSet = Assert.IsType<DataSet>(Assert.Single(output));

        Assert.Single(dataSet.Tables);
        Assert.Equal("Ada", dataSet.Tables[0].Rows[0]["Name"]);
    }

    [Fact]
    public void WriteRows_DataTableReturnType_CopiesDetachedRows()
    {
        using var rowOwner = new DataTable("Rows");
        var row = CreateDetachedRow(rowOwner, "Ada");
        var output = new List<object?>();

        DbaXResultWriter.WriteRows(
            new[] { row },
            ReturnType.DataTable,
            (value, _) => output.Add(value));

        using var table = Assert.IsType<DataTable>(Assert.Single(output));

        Assert.Single(table.Rows);
        Assert.Equal("Ada", table.Rows[0]["Name"]);
    }

    [Fact]
    public void WriteRows_DataSetReturnType_CopiesDetachedRows()
    {
        using var rowOwner = new DataTable("Rows");
        var row = CreateDetachedRow(rowOwner, "Ada");
        var output = new List<object?>();

        DbaXResultWriter.WriteRows(
            new[] { row },
            ReturnType.DataSet,
            (value, _) => output.Add(value));

        using var dataSet = Assert.IsType<DataSet>(Assert.Single(output));

        Assert.Single(dataSet.Tables);
        Assert.Single(dataSet.Tables[0].Rows);
        Assert.Equal("Ada", dataSet.Tables[0].Rows[0]["Name"]);
    }

    [Fact]
    public async Task WriteRowsAsync_DataTableReturnType_CopiesDetachedRows()
    {
        using var rowOwner = new DataTable("Rows");
        var row = CreateDetachedRow(rowOwner, "Ada");
        var output = new List<object?>();

        await DbaXResultWriter.WriteRowsAsync(
            StreamRows(row),
            ReturnType.DataTable,
            (value, _) => output.Add(value));

        using var table = Assert.IsType<DataTable>(Assert.Single(output));

        Assert.Single(table.Rows);
        Assert.Equal("Ada", table.Rows[0]["Name"]);
    }

    [Fact]
    public async Task WriteRowsAsync_DataSetReturnType_CopiesDetachedRows()
    {
        using var rowOwner = new DataTable("Rows");
        var row = CreateDetachedRow(rowOwner, "Ada");
        var output = new List<object?>();

        await DbaXResultWriter.WriteRowsAsync(
            StreamRows(row),
            ReturnType.DataSet,
            (value, _) => output.Add(value));

        using var dataSet = Assert.IsType<DataSet>(Assert.Single(output));

        Assert.Single(dataSet.Tables);
        Assert.Single(dataSet.Tables[0].Rows);
        Assert.Equal("Ada", dataSet.Tables[0].Rows[0]["Name"]);
    }

    [Fact]
    public async Task WriteRowsAsync_PSObjectReturnType_ProjectsDetachedRows()
    {
        using var rowOwner = new DataTable("Rows");
        var row = CreateDetachedRow(rowOwner, "Ada");
        var output = new List<object?>();

        await DbaXResultWriter.WriteRowsAsync(
            StreamRows(row),
            ReturnType.PSObject,
            (value, _) => output.Add(value));

        var psObject = Assert.IsType<PSObject>(Assert.Single(output));

        Assert.Equal("Ada", psObject.Properties["Name"].Value);
    }

    [Fact]
    public void WriteRows_DataTableReturnType_WidensAcrossSourceTables()
    {
        using var integers = new DataTable();
        integers.Columns.Add("Value", typeof(long));
        using var texts = new DataTable();
        texts.Columns.Add("Value", typeof(string));
        var first = integers.NewRow();
        first["Value"] = 1L;
        var second = texts.NewRow();
        second["Value"] = "a";
        var output = new List<object?>();

        DbaXResultWriter.WriteRows(
            new[] { first, second },
            ReturnType.DataTable,
            (value, _) => output.Add(value));

        using var table = Assert.IsType<DataTable>(Assert.Single(output));

        Assert.Equal("Table0", table.TableName);
        Assert.Equal(typeof(object), table.Columns["Value"]!.DataType);
        Assert.Equal(new object[] { 1L, "a" }, table.Rows.Cast<DataRow>().Select(row => row["Value"]).ToArray());
    }

    private static DataRow CreateDetachedRow(DataTable table, string name)
    {
        table.Columns.Add("Name", typeof(string));
        var row = table.NewRow();
        row["Name"] = name;
        return row;
    }

    private static async IAsyncEnumerable<DataRow> StreamRows(params DataRow[] rows)
    {
        foreach (var row in rows)
        {
            await Task.Yield();
            yield return row;
        }
    }
}
