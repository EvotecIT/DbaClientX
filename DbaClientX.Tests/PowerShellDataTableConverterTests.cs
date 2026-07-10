using System.Data;
using DBAClientX.PowerShell;

namespace DbaClientX.Tests;

public class PowerShellDataTableConverterTests
{
    [Fact]
    public void ToDataTable_ExpandsSingleEnumerableInput()
    {
        var rows = new[]
        {
            new ConverterRow { Id = 1, Name = "Alpha" },
            new ConverterRow { Id = 2, Name = "Beta" }
        };

        var table = PowerShellDataTableConverter.ToDataTable(new object?[] { rows }, "Records");

        Assert.Equal(typeof(int), table.Columns["Id"]!.DataType);
        Assert.Equal(typeof(string), table.Columns["Name"]!.DataType);
        Assert.Equal(2, table.Rows.Count);
        Assert.Equal("Beta", table.Rows[1]["Name"]);
    }

    [Fact]
    public void ToDataTable_DiscoversColumnsAcrossDictionaryRows()
    {
        var table = PowerShellDataTableConverter.ToDataTable(new object?[]
        {
            new Dictionary<string, object?>(StringComparer.Ordinal) { ["Name"] = "Alpha" },
            new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
            {
                ["name"] = "Beta",
                ["Age"] = 42
            }
        }, "Records");

        Assert.Equal(new[] { "Name", "Age" }, table.Columns.Cast<DataColumn>().Select(column => column.ColumnName));
        Assert.Equal("Beta", table.Rows[1]["Name"]);
        Assert.Equal(42, table.Rows[1]["Age"]);
    }

    [Fact]
    public void ToDataTable_ReturnsDirectDataTableInput()
    {
        var source = new DataTable("Source");
        source.Columns.Add("Name", typeof(string));
        source.Rows.Add("Alpha");

        var table = PowerShellDataTableConverter.ToDataTable(new object?[] { source }, "Records");

        Assert.Same(source, table);
        Assert.Equal("Alpha", table.Rows[0]["Name"]);
    }

    [Fact]
    public void ToDataTable_PreservesDataRecordFieldTypes()
    {
        using var source = new DataTable();
        source.Columns.Add("Id", typeof(int));
        source.Columns.Add("Score", typeof(decimal));
        source.Columns.Add("CreatedUtc", typeof(DateTime));
        source.Rows.Add(1, 12.5m, new DateTime(2026, 6, 27, 12, 0, 0, DateTimeKind.Utc));

        using var reader = source.CreateDataReader();
        Assert.True(reader.Read());

        var table = PowerShellDataTableConverter.ToDataTable(new object?[] { reader, reader }, "Records");

        Assert.Equal(typeof(int), table.Columns["Id"]!.DataType);
        Assert.Equal(typeof(decimal), table.Columns["Score"]!.DataType);
        Assert.Equal(typeof(DateTime), table.Columns["CreatedUtc"]!.DataType);
        Assert.Equal(1, table.Rows[0]["Id"]);
        Assert.Equal(12.5m, table.Rows[0]["Score"]);
    }

    private sealed class ConverterRow
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }
}
