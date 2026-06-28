using System.Data;
using DBAClientX.PowerShell;

namespace DbaClientX.Tests;

public class PowerShellDataTableConverterTests
{
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
}
