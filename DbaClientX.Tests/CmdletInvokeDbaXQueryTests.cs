using System.Data;
using System.Reflection;
using System.Management.Automation;
using DBAClientX.PowerShell;
using DBAClientX;
using Xunit;

public class CmdletInvokeDbaXQueryTests
{
    [Fact]
    public void DataRowToPSObject_MapsColumnsToProperties()
    {
        var table = new DataTable();
        table.Columns.Add("id", typeof(int));
        table.Columns.Add("name", typeof(string));
        var row = table.NewRow();
        row["id"] = 1;
        row["name"] = "one";
        table.Rows.Add(row);

        var cmdlet = new CmdletIInvokeDbaXQuery
        {
            ReturnType = ReturnType.PSObject
        };

        var method = typeof(CmdletIInvokeDbaXQuery).GetMethod("DataRowToPSObject", BindingFlags.NonPublic | BindingFlags.Static);
        var psObject = (PSObject)method!.Invoke(null, new object[] { row })!;

        Assert.Equal(1, (int)psObject.Properties["id"].Value);
        Assert.Equal("one", (string)psObject.Properties["name"].Value);
    }
}
