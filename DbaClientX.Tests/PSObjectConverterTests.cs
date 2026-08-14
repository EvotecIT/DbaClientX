using System.Data;
using System.Management.Automation;
using DBAClientX.PowerShell;
using Xunit;

public class PSObjectConverterTests
{
    [Fact]
    public void DataRowToPSObjectCreatesProperties()
    {
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("Name", typeof(string));
        var row1 = table.NewRow();
        row1["Id"] = 1;
        row1["Name"] = "Alice";
        table.Rows.Add(row1);
        var row2 = table.NewRow();
        row2["Id"] = 2;
        row2["Name"] = "Bob";
        table.Rows.Add(row2);

        var ps1 = PSObjectConverter.DataRowToPSObject(row1);
        Assert.Equal(1, ps1.Properties["Id"].Value);
        Assert.Equal("Alice", ps1.Properties["Name"].Value);

        var ps2 = PSObjectConverter.DataRowToPSObject(row2);
        Assert.Equal(2, ps2.Properties["Id"].Value);
        Assert.Equal("Bob", ps2.Properties["Name"].Value);
    }

    [Fact]
    public void DataRecordToPSObjectPreservesValuesAndConvertsDbNullToNull()
    {
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("Name", typeof(string));
        table.Rows.Add(7, DBNull.Value);

        using var reader = table.CreateDataReader();
        Assert.True(reader.Read());
        string[] columnNames = PSObjectConverter.GetUniqueColumnNames(reader);
        var converted = PSObjectConverter.DataRecordToPSObject(reader, columnNames, new object[reader.FieldCount]);

        Assert.Equal(7, converted.Properties["Id"].Value);
        Assert.Null(converted.Properties["Name"].Value);
    }

    [Fact]
    public void GetUniqueColumnNamesPreservesWhitespaceAliases()
    {
        var table = new DataTable();
        table.Columns.Add(" ", typeof(int));
        table.Rows.Add(1);

        using var reader = table.CreateDataReader();
        Assert.Equal(new[] { " " }, PSObjectConverter.GetUniqueColumnNames(reader));
    }

    [Fact]
    public void DataRowToPSObjectCanonicalizesReservedPowerShellMemberNamesWithoutHidingOrdinaryColumns()
    {
        var table = new DataTable();
        table.Columns.Add("Column_PSObject", typeof(int));
        table.Columns.Add("PSObject", typeof(int));
        table.Columns.Add("PSBase", typeof(int));
        table.Rows.Add(1, 2, 3);

        var converted = PSObjectConverter.DataRowToPSObject(table.Rows[0]);

        Assert.Equal(1, converted.Properties["Column_PSObject"].Value);
        Assert.Equal(2, converted.Properties["Column_PSObject1"].Value);
        Assert.Equal(3, converted.Properties["Column_PSBase"].Value);
    }

    [Fact]
    public void DataRecordToPSObjectCanonicalizesReservedPowerShellMemberNames()
    {
        var table = new DataTable();
        table.Columns.Add("PSObject", typeof(int));
        table.Columns.Add("PSFoo", typeof(int));
        table.Rows.Add(4, 5);

        using var reader = table.CreateDataReader();
        Assert.True(reader.Read());
        string[] columnNames = PSObjectConverter.GetUniqueColumnNames(reader);
        var converted = PSObjectConverter.DataRecordToPSObject(reader, columnNames, new object[reader.FieldCount]);

        Assert.Equal(new[] { "Column_PSObject", "Column_PSFoo" }, columnNames);
        Assert.Equal(4, converted.Properties["Column_PSObject"].Value);
        Assert.Equal(5, converted.Properties["Column_PSFoo"].Value);
    }
}
