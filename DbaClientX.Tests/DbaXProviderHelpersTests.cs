using System.Data;
using DBAClientX.PowerShell;

namespace DbaClientX.Tests;

public class DbaXProviderHelpersTests
{
    [Theory]
    [InlineData(@"Data Source=C:\data\app.db", @"C:\data\app.db")]
    [InlineData(@"Filename=C:\data\app.db", @"C:\data\app.db")]
    [InlineData(@"C:\data\app.db", @"C:\data\app.db")]
    public void GetSQLiteDatabase_ParsesSingleKeyConnectionStrings(string input, string expected)
    {
        var database = DbaXProviderHelpers.GetSQLiteDatabase(input);

        Assert.Equal(expected, database);
    }

    [Fact]
    public void NormalizeBulkInsertInput_NormalizesPostgreSqlDestinationAndColumns()
    {
        var table = new DataTable("Users");
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("DisplayName", typeof(string));
        table.Rows.Add(1, "Ada");

        var (bulkTable, destinationTable) = DbaXProviderHelpers.NormalizeBulkInsertInput(
            DbaXProvider.PostgreSql,
            table,
            "public.Users");

        using var disposableBulkTable = ReferenceEquals(bulkTable, table) ? null : bulkTable;

        Assert.Equal("public.users", destinationTable);
        Assert.NotSame(table, bulkTable);
        Assert.Equal(
            new[] { "id", "displayname" },
            bulkTable.Columns.Cast<DataColumn>().Select(static column => column.ColumnName));
    }

    [Fact]
    public void NormalizeBulkInsertInput_LeavesNonPostgreSqlInputUnchanged()
    {
        var table = new DataTable("Users");
        table.Columns.Add("Id", typeof(int));

        var (bulkTable, destinationTable) = DbaXProviderHelpers.NormalizeBulkInsertInput(
            DbaXProvider.SqlServer,
            table,
            "dbo.Users");

        Assert.Same(table, bulkTable);
        Assert.Equal("dbo.Users", destinationTable);
    }
}
