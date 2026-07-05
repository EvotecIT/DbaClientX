using DBAClientX.Metadata;
using DBAClientX.PowerShell;

namespace DbaClientX.Tests;

public class CmdletGetDbaXTableCopyPlanTests
{
    [Fact]
    public void FilterSourceTables_AppliesSourceTableAndSchema()
    {
        var tables = new[]
        {
            new DbaTableInfo("dbo", "Users", DbaTableKind.Table),
            new DbaTableInfo("dbo", "Groups", DbaTableKind.Table),
            new DbaTableInfo("audit", "Users", DbaTableKind.Table)
        };

        var filtered = CmdletGetDbaXTableCopyPlan.FilterSourceTables(tables, "dbo", "Users");

        var table = Assert.Single(filtered);
        Assert.Equal("dbo", table.Schema);
        Assert.Equal("Users", table.Name);
    }

    [Fact]
    public void FilterSourceTables_LeavesAllTablesWithoutSourceTable()
    {
        var tables = new[]
        {
            new DbaTableInfo("dbo", "Users", DbaTableKind.Table),
            new DbaTableInfo("dbo", "Groups", DbaTableKind.Table)
        };

        var filtered = CmdletGetDbaXTableCopyPlan.FilterSourceTables(tables, "dbo", null);

        Assert.Equal(2, filtered.Count);
    }

    [Fact]
    public void GetDestinationColumnSchemaFilter_FetchesAllColumnsWhenMappingsExist()
    {
        var filter = CmdletGetDbaXTableCopyPlan.GetDestinationColumnSchemaFilter(
            "archive",
            new Dictionary<string, string> { ["Users"] = "custom.Users" });

        Assert.Null(filter);
    }

    [Fact]
    public void GetDestinationColumnSchemaFilter_KeepsDestinationSchemaWithoutMappings()
    {
        var filter = CmdletGetDbaXTableCopyPlan.GetDestinationColumnSchemaFilter("archive", null);

        Assert.Equal("archive", filter);
    }
}
