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
    public void GetSQLiteDatabase_ParsesFullUriConnectionStrings()
    {
        var path = Path.Combine(Path.GetTempPath(), "dbaclientx-fulluri.db");
        var database = DbaXProviderHelpers.GetSQLiteDatabase("FullUri=" + new Uri(path).AbsoluteUri);

        Assert.Equal(path, database);
    }

    [Theory]
    [InlineData(":memory:", false)]
    [InlineData("Data Source=:memory:", false)]
    [InlineData(@"C:\data\app.db", true)]
    [InlineData(@"Data Source=C:\data\app.db", true)]
    public void IsSQLiteFileBackedDatabase_DetectsFileBackedTargets(string input, bool expected)
    {
        var actual = DbaXProviderHelpers.IsSQLiteFileBackedDatabase(input);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void ExecutePing_SQLiteMissingFile_DoesNotCreateDatabase()
    {
        var path = Path.Combine(Path.GetTempPath(), "dbaclientx-missing-" + Guid.NewGuid().ToString("N") + ".db");

        var exception = Assert.Throws<InvalidOperationException>(() => DbaXProviderHelpers.ExecutePing(DbaXProvider.SQLite, path));

        Assert.Contains("does not exist", exception.Message);
        Assert.False(File.Exists(path));
    }

    [Fact]
    public void NormalizeBulkInsertInput_NormalizesPostgreSqlDestinationAndColumns()
    {
        using var table = new DataTable("Users");
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
        using var table = new DataTable("Users");
        table.Columns.Add("Id", typeof(int));

        var (bulkTable, destinationTable) = DbaXProviderHelpers.NormalizeBulkInsertInput(
            DbaXProvider.SqlServer,
            table,
            "dbo.Users");

        Assert.Same(table, bulkTable);
        Assert.Equal("dbo.Users", destinationTable);
    }
}
