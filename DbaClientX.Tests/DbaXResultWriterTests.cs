using System.Data;
using DBAClientX;
using DBAClientX.PowerShell;

namespace DbaClientX.Tests;

public class DbaXResultWriterTests
{
    [Fact]
    public void WriteRows_DataSetReturnType_WritesDataSet()
    {
        var table = new DataTable("Rows");
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
}
