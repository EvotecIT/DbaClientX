using System.Data;
using DBAClientX;

namespace DbaClientX.Tests;

public class DbaParameterValueExecutionTests
{
    [Fact]
    public async Task QueryAsync_SqliteMetadataValues_PreservesUnicodeBinaryAndNull()
    {
        using var provider = new SQLite { ReturnType = ReturnType.DataTable };
        var result = await provider.QueryAsync(":memory:",
            "SELECT @text AS TextValue, @binary AS BinaryValue, @missing AS MissingValue;",
            new Dictionary<string, object?>
            {
                ["@text"] = new DbaParameterValue("é漢字", DbType.String, 160),
                ["@binary"] = new DbaParameterValue(new byte[] { 0, 128, 255 }, DbType.Binary, 3),
                ["@missing"] = new DbaParameterValue(null, DbType.Int32)
            });

        var table = Assert.IsType<DataTable>(result);
        var row = Assert.Single(table.Rows.Cast<DataRow>());
        Assert.Equal("é漢字", row["TextValue"]);
        Assert.Equal(new byte[] { 0, 128, 255 }, Assert.IsType<byte[]>(row["BinaryValue"]));
        Assert.Same(DBNull.Value, row["MissingValue"]);
    }

    [Fact]
    public async Task QueryAsync_SqlServerMetadataValues_PreservesUnicodeMaxBinaryAndNull()
    {
        await using var fixture = await SqlServerPlanTestScope.CreateAsync();
        using var provider = new SqlServer { ReturnType = ReturnType.DataTable };
        string large = new string('é', 5001) + "漢字";
        var result = await provider.QueryAsync(fixture.Connection,
            "SELECT @text AS TextValue, @large AS LargeValue, @binary AS BinaryValue, @missing AS MissingValue;",
            new Dictionary<string, object?>
            {
                ["@text"] = new DbaParameterValue("é漢字", DbType.String, 160),
                ["@large"] = new DbaParameterValue(large, DbType.String, -1),
                ["@binary"] = new DbaParameterValue(new byte[] { 0, 128, 255 }, DbType.Binary, 3),
                ["@missing"] = new DbaParameterValue(null, DbType.Int32)
            });

        var table = Assert.IsType<DataTable>(result);
        var row = Assert.Single(table.Rows.Cast<DataRow>());
        Assert.Equal("é漢字", row["TextValue"]);
        Assert.Equal(large, row["LargeValue"]);
        Assert.Equal(new byte[] { 0, 128, 255 }, Assert.IsType<byte[]>(row["BinaryValue"]));
        Assert.Same(DBNull.Value, row["MissingValue"]);
    }
}
