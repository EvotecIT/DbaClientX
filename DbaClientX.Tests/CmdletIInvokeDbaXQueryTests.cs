using System.Data;
using System.Management.Automation;
using System.Reflection;
using DBAClientX.PowerShell;
using Xunit;

public class CmdletIInvokeDbaXQueryTests
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

        var method = typeof(CmdletIInvokeDbaXQuery).GetMethod("DataRowToPSObject", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);

        var ps1 = (PSObject)method!.Invoke(null, new object[] { row1 })!;
        Assert.Equal(1, ps1.Properties["Id"].Value);
        Assert.Equal("Alice", ps1.Properties["Name"].Value);

        var ps2 = (PSObject)method.Invoke(null, new object[] { row2 })!;
        Assert.Equal(2, ps2.Properties["Id"].Value);
        Assert.Equal("Bob", ps2.Properties["Name"].Value);
    }
}
