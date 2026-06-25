using System.Reflection;
using DBAClientX.PowerShell;

namespace DbaClientX.Tests;

public class DbaXTableCopyAdapterTests
{
    [Fact]
    public void BuildPageQuery_OracleFoldsSimpleIdentifiersToUppercase()
    {
        var adapter = new DbaXTableCopyAdapter(
            DbaXBulkProvider.Oracle,
            "Data Source=oracle;User Id=u;Password=p",
            new[] { "Id" });

        var query = InvokeBuildPageQuery(adapter, "app.Users", 0, 10);

        Assert.Equal("SELECT * FROM APP.USERS ORDER BY ID OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY", query);
    }

    [Fact]
    public void BuildPageQuery_OracleQuotesNonSimpleIdentifiers()
    {
        var adapter = new DbaXTableCopyAdapter(
            DbaXBulkProvider.Oracle,
            "Data Source=oracle;User Id=u;Password=p",
            new[] { "Created At" });

        var query = InvokeBuildPageQuery(adapter, "app.User Audit", 5, 10);

        Assert.Equal("SELECT * FROM APP.\"User Audit\" ORDER BY \"Created At\" OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY", query);
    }

    private static string InvokeBuildPageQuery(DbaXTableCopyAdapter adapter, string tableName, long offset, int pageSize)
    {
        var method = typeof(DbaXTableCopyAdapter).GetMethod("BuildPageQuery", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(nameof(DbaXTableCopyAdapter), "BuildPageQuery");

        return (string)method.Invoke(adapter, new object[] { tableName, offset, pageSize })!;
    }
}
