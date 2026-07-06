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

        var filtered = CmdletGetDbaXTableCopyPlan.FilterSourceTables(DbaXProvider.SqlServer, tables, "dbo", "Users");

        var table = Assert.Single(filtered);
        Assert.Equal("dbo", table.Schema);
        Assert.Equal("Users", table.Name);
    }

    [Fact]
    public void ResolveSourceTableFilter_SplitsQualifiedSourceTable()
    {
        var filter = CmdletGetDbaXTableCopyPlan.ResolveSourceTableFilter(null, "dbo.Users");

        Assert.Equal("dbo", filter.SourceSchema);
        Assert.Equal("Users", filter.SourceTable);
    }

    [Fact]
    public void ResolveSourceTableFilter_PreservesExplicitSourceSchema()
    {
        var filter = CmdletGetDbaXTableCopyPlan.ResolveSourceTableFilter("audit", "dbo.Users");

        Assert.Equal("audit", filter.SourceSchema);
        Assert.Equal("Users", filter.SourceTable);
    }

    [Fact]
    public void FilterSourceTables_AppliesQualifiedSourceTable()
    {
        var tables = new[]
        {
            new DbaTableInfo("dbo", "Users", DbaTableKind.Table),
            new DbaTableInfo("audit", "Users", DbaTableKind.Table)
        };
        var (schema, table) = CmdletGetDbaXTableCopyPlan.ResolveSourceTableFilter(null, "dbo.Users");

        var filtered = CmdletGetDbaXTableCopyPlan.FilterSourceTables(DbaXProvider.SqlServer, tables, schema, table);

        var match = Assert.Single(filtered);
        Assert.Equal("dbo", match.Schema);
        Assert.Equal("Users", match.Name);
    }

    [Fact]
    public void FilterSourceTables_LeavesAllTablesWithoutSourceTable()
    {
        var tables = new[]
        {
            new DbaTableInfo("dbo", "Users", DbaTableKind.Table),
            new DbaTableInfo("dbo", "Groups", DbaTableKind.Table)
        };

        var filtered = CmdletGetDbaXTableCopyPlan.FilterSourceTables(DbaXProvider.SqlServer, tables, "dbo", null);

        Assert.Equal(2, filtered.Count);
    }

    [Fact]
    public void FilterSourceTables_PostgreSqlUsesProviderIdentifierSemantics()
    {
        var tables = new[]
        {
            new DbaTableInfo("public", "users", DbaTableKind.Table),
            new DbaTableInfo("public", "Users", DbaTableKind.Table)
        };

        var filtered = CmdletGetDbaXTableCopyPlan.FilterSourceTables(DbaXProvider.PostgreSql, tables, "public", "users");

        var table = Assert.Single(filtered);
        Assert.Equal("users", table.Name);
    }

    [Fact]
    public void FilterSourceTables_OracleUsesExactOrUppercaseIdentifierSemantics()
    {
        var tables = new[]
        {
            new DbaTableInfo("APP", "USERS", DbaTableKind.Table),
            new DbaTableInfo("APP", "Users", DbaTableKind.Table)
        };

        var filtered = CmdletGetDbaXTableCopyPlan.FilterSourceTables(DbaXProvider.Oracle, tables, "APP", "USERS");

        var table = Assert.Single(filtered);
        Assert.Equal("USERS", table.Name);
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
