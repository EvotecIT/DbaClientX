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

        var dataSet = Assert.IsType<DataSet>(Assert.Single(output));

        Assert.Single(dataSet.Tables);
        Assert.Equal("Ada", dataSet.Tables[0].Rows[0]["Name"]);
    }

    [Fact]
    public void WriteRows_DataTableReturnType_CopiesDetachedRows()
    {
        var (row, rowOwner) = CreateDetachedRow("Ada");
        using var _ = rowOwner;
        var output = new List<object?>();

        DbaXResultWriter.WriteRows(
            new[] { row },
            ReturnType.DataTable,
            (value, _) => output.Add(value));

        var table = Assert.IsType<DataTable>(Assert.Single(output));

        Assert.Single(table.Rows);
        Assert.Equal("Ada", table.Rows[0]["Name"]);
    }

    [Fact]
    public void WriteRows_DataSetReturnType_CopiesDetachedRows()
    {
        var (row, rowOwner) = CreateDetachedRow("Ada");
        using var _ = rowOwner;
        var output = new List<object?>();

        DbaXResultWriter.WriteRows(
            new[] { row },
            ReturnType.DataSet,
            (value, _) => output.Add(value));

        var dataSet = Assert.IsType<DataSet>(Assert.Single(output));

        Assert.Single(dataSet.Tables);
        Assert.Single(dataSet.Tables[0].Rows);
        Assert.Equal("Ada", dataSet.Tables[0].Rows[0]["Name"]);
    }

    [Fact]
    public async Task WriteRowsAsync_DataTableReturnType_CopiesDetachedRows()
    {
        var (row, rowOwner) = CreateDetachedRow("Ada");
        using var _ = rowOwner;
        var output = new List<object?>();

        await DbaXResultWriter.WriteRowsAsync(
            StreamRows(row),
            ReturnType.DataTable,
            (value, _) => output.Add(value));

        var table = Assert.IsType<DataTable>(Assert.Single(output));

        Assert.Single(table.Rows);
        Assert.Equal("Ada", table.Rows[0]["Name"]);
    }

    [Fact]
    public async Task WriteRowsAsync_DataSetReturnType_CopiesDetachedRows()
    {
        var (row, rowOwner) = CreateDetachedRow("Ada");
        using var _ = rowOwner;
        var output = new List<object?>();

        await DbaXResultWriter.WriteRowsAsync(
            StreamRows(row),
            ReturnType.DataSet,
            (value, _) => output.Add(value));

        var dataSet = Assert.IsType<DataSet>(Assert.Single(output));

        Assert.Single(dataSet.Tables);
        Assert.Single(dataSet.Tables[0].Rows);
        Assert.Equal("Ada", dataSet.Tables[0].Rows[0]["Name"]);
    }

    [Fact]
    public async Task WriteRowsAsync_PSObjectReturnType_ProjectsDetachedRows()
    {
        var (row, rowOwner) = CreateDetachedRow("Ada");
        using var _ = rowOwner;
        var output = new List<object?>();

        await DbaXResultWriter.WriteRowsAsync(
            StreamRows(row),
            ReturnType.PSObject,
            (value, _) => output.Add(value));

        var psObject = Assert.IsType<PSObject>(Assert.Single(output));

        Assert.Equal("Ada", psObject.Properties["Name"].Value);
    }

    private static (DataRow Row, DataTable Owner) CreateDetachedRow(string name)
    {
        var table = new DataTable("Rows");
        table.Columns.Add("Name", typeof(string));
        var row = table.NewRow();
        row["Name"] = name;
        return (row, table);
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
