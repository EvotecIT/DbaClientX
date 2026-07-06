using System.Data;
using System.Management.Automation;
using DBAClientX.PowerShell;
using Microsoft.Data.Sqlite;

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
        var path = Path.Join(Path.GetTempPath(), "dbaclientx-fulluri.db");
        var database = DbaXProviderHelpers.GetSQLiteDatabase("FullUri=" + new Uri(path).AbsoluteUri);

        Assert.Equal(path, database);
    }

    [Fact]
    public void GetSQLiteDatabase_PreservesOptionBearingConnectionStrings()
    {
        const string connectionString = "Data Source=shared;Mode=Memory;Cache=Shared";

        var database = DbaXProviderHelpers.GetSQLiteDatabase(connectionString);

        Assert.Equal(connectionString, database);
    }

    [Fact]
    public void GetSQLiteDatabase_PreservesFullUriQueryOptions()
    {
        var path = Path.Join(Path.GetTempPath(), "dbaclientx-fulluri-options.db");
        var connectionString = "FullUri=" + new Uri(path).AbsoluteUri + "?mode=ro&cache=shared";

        var database = DbaXProviderHelpers.GetSQLiteDatabase(connectionString);

        Assert.Equal(connectionString, database);
    }

    [Fact]
    public void GetSQLiteConnectionString_PreservesConnectionStringOptions()
    {
        const string connectionString = "Data Source=shared;Mode=Memory;Cache=Shared";

        var actual = DbaXProviderHelpers.GetSQLiteConnectionString(connectionString);

        Assert.Equal(connectionString, actual);
    }

    [Fact]
    public void GetSQLiteConnectionString_PreservesOneKeyOptionsForValidation()
    {
        const string connectionString = "Mode=ReadOnly";

        var actual = DbaXProviderHelpers.GetSQLiteConnectionString(connectionString);

        Assert.Equal(connectionString, actual);
    }

    [Fact]
    public void GetSQLiteConnectionString_BuildsRawDatabasePaths()
    {
        const string path = "data.db";

        var actual = DbaXProviderHelpers.GetSQLiteConnectionString(path);

        Assert.Contains("Data Source=data.db", actual);
        Assert.Contains("Pooling=False", actual);
    }

    [Fact]
    public void GetSQLiteConnectionString_BuildsRawDatabasePathsContainingEqualsSigns()
    {
        const string path = "app=prod.db";

        var actual = DbaXProviderHelpers.GetSQLiteConnectionString(path);
        var builder = new SqliteConnectionStringBuilder(actual);

        Assert.Equal(path, builder.DataSource);
    }

    [Fact]
    public void GetSQLiteReadOnlyConnectionString_BuildsRawDatabasePaths()
    {
        const string path = "data.db";

        var actual = DbaXProviderHelpers.GetSQLiteReadOnlyConnectionString(path);
        var builder = new SqliteConnectionStringBuilder(actual);

        Assert.Equal(path, builder.DataSource);
        Assert.Equal(SqliteOpenMode.ReadOnly, builder.Mode);
        Assert.False(builder.Pooling);
    }

    [Fact]
    public void GetSQLiteReadOnlyConnectionString_RebuildsSingleKeyConnectionStrings()
    {
        const string connectionString = "Data Source=data.db";

        var actual = DbaXProviderHelpers.GetSQLiteReadOnlyConnectionString(connectionString);
        var builder = new SqliteConnectionStringBuilder(actual);

        Assert.Equal("data.db", builder.DataSource);
        Assert.Equal(SqliteOpenMode.ReadOnly, builder.Mode);
        Assert.False(builder.Pooling);
    }

    [Fact]
    public void GetSQLiteReadOnlyConnectionString_PreservesOptionBearingConnectionStrings()
    {
        const string connectionString = "Data Source=shared;Mode=Memory;Cache=Shared";

        var actual = DbaXProviderHelpers.GetSQLiteReadOnlyConnectionString(connectionString);

        Assert.Equal(connectionString, actual);
    }

    [Fact]
    public void GetSQLiteReadOnlyConnectionString_ForcesReadOnlyForOptionBearingFileConnectionStrings()
    {
        const string connectionString = @"Data Source=C:\data\app.db;Default Timeout=5";

        var actual = DbaXProviderHelpers.GetSQLiteReadOnlyConnectionString(connectionString);
        var builder = new SqliteConnectionStringBuilder(actual);

        Assert.Equal(@"C:\data\app.db", builder.DataSource);
        Assert.Equal(SqliteOpenMode.ReadOnly, builder.Mode);
        Assert.Equal(5, builder.DefaultTimeout);
    }

    [Fact]
    public void GetSQLiteReadOnlyConnectionString_ForcesReadOnlyForFullUriFileConnectionStrings()
    {
        var path = Path.Join(Path.GetTempPath(), "dbaclientx-fulluri-readonly.db");
        var connectionString = "FullUri=" + new Uri(path).AbsoluteUri;

        var actual = DbaXProviderHelpers.GetSQLiteReadOnlyConnectionString(connectionString);
        var builder = new SqliteConnectionStringBuilder(actual);

        Assert.Equal(path, builder.DataSource);
        Assert.Equal(SqliteOpenMode.ReadOnly, builder.Mode);
        Assert.False(builder.Pooling);
    }

    [Fact]
    public void GetSQLiteReadOnlyConnectionString_ForcesReadOnlyAndPreservesFullUriCache()
    {
        var path = Path.Join(Path.GetTempPath(), "dbaclientx-fulluri-cache.db");
        var connectionString = "FullUri=" + new Uri(path).AbsoluteUri + "?cache=shared";

        var actual = DbaXProviderHelpers.GetSQLiteReadOnlyConnectionString(connectionString);
        var builder = new SqliteConnectionStringBuilder(actual);

        Assert.Equal(path, builder.DataSource);
        Assert.Equal(SqliteOpenMode.ReadOnly, builder.Mode);
        Assert.Equal(SqliteCacheMode.Shared, builder.Cache);
    }

    [Fact]
    public void GetSQLiteReadOnlyConnectionString_ForcesReadOnlyAndPreservesDataSourceUriCache()
    {
        var path = Path.Join(Path.GetTempPath(), "dbaclientx-datasource-uri-cache.db");
        var connectionString = "Data Source=" + new Uri(path).AbsoluteUri + "?cache=shared";

        var actual = DbaXProviderHelpers.GetSQLiteReadOnlyConnectionString(connectionString);
        var builder = new SqliteConnectionStringBuilder(actual);

        Assert.Equal(path, builder.DataSource);
        Assert.Equal(SqliteOpenMode.ReadOnly, builder.Mode);
        Assert.Equal(SqliteCacheMode.Shared, builder.Cache);
    }

    [Fact]
    public void GetSQLiteReadOnlyConnectionString_PreservesFullUriMemoryMode()
    {
        var path = Path.Join(Path.GetTempPath(), "dbaclientx-fulluri-memory.db");
        var connectionString = "FullUri=" + new Uri(path).AbsoluteUri + "?mode=memory&cache=shared";

        var actual = DbaXProviderHelpers.GetSQLiteReadOnlyConnectionString(connectionString);

        Assert.Equal(connectionString, actual);
    }

    [Fact]
    public void GetSQLiteReadOnlyConnectionString_PreservesDataSourceUriMemoryMode()
    {
        var path = Path.Join(Path.GetTempPath(), "dbaclientx-datasource-uri-memory.db");
        var connectionString = "Data Source=" + new Uri(path).AbsoluteUri + "?mode=memory&cache=shared";

        var actual = DbaXProviderHelpers.GetSQLiteReadOnlyConnectionString(connectionString);

        Assert.Equal(connectionString, actual);
    }

    [Fact]
    public void IsSQLiteFileBackedDatabase_TreatsFullUriMemoryModeAsMemory()
    {
        var path = Path.Join(Path.GetTempPath(), "dbaclientx-fulluri-memory-probe.db");
        var connectionString = "FullUri=" + new Uri(path).AbsoluteUri + "?mode=memory&cache=shared";

        var fileBacked = DbaXProviderHelpers.IsSQLiteFileBackedDatabase(connectionString);

        Assert.False(fileBacked);
    }

    [Fact]
    public void IsSQLiteFileBackedDatabase_TreatsDataSourceUriMemoryModeAsMemory()
    {
        var path = Path.Join(Path.GetTempPath(), "dbaclientx-datasource-uri-memory-probe.db");
        var connectionString = "Data Source=" + new Uri(path).AbsoluteUri + "?mode=memory&cache=shared";

        var fileBacked = DbaXProviderHelpers.IsSQLiteFileBackedDatabase(connectionString);

        Assert.False(fileBacked);
    }

    [Fact]
    public void GetSQLiteReadOnlyConnectionString_PreservesOneKeyOptionsForValidation()
    {
        const string connectionString = "Mode=ReadOnly";

        var actual = DbaXProviderHelpers.GetSQLiteReadOnlyConnectionString(connectionString);

        Assert.Equal(connectionString, actual);
    }

    [Fact]
    public void GetSQLiteDatabasePath_ResolvesOptionBearingFileConnectionStrings()
    {
        const string connectionString = @"Data Source=C:\data\app.db;Default Timeout=5";

        var actual = DbaXProviderHelpers.GetSQLiteDatabasePath(connectionString, "SQLite maintenance");

        Assert.Equal(@"C:\data\app.db", actual);
    }

    [Fact]
    public void GetSQLiteDatabasePath_RejectsMemoryConnectionStrings()
    {
        const string connectionString = "Data Source=shared;Mode=Memory;Cache=Shared";

        var exception = Assert.Throws<PSArgumentException>(() => DbaXProviderHelpers.GetSQLiteDatabasePath(connectionString, "SQLite maintenance"));

        Assert.Contains("file-backed", exception.Message);
    }

    [Fact]
    public void GetSQLiteDatabasePath_RejectsFullUriMemoryConnectionStrings()
    {
        var path = Path.Join(Path.GetTempPath(), "dbaclientx-fulluri-memory-path.db");
        var connectionString = "FullUri=" + new Uri(path).AbsoluteUri + "?mode=memory&cache=shared";

        var exception = Assert.Throws<PSArgumentException>(() => DbaXProviderHelpers.GetSQLiteDatabasePath(connectionString, "SQLite maintenance"));

        Assert.Contains("file-backed", exception.Message);
    }

    [Fact]
    public void GetSQLiteDatabasePath_RejectsNonFileFullUriConnectionStrings()
    {
        const string connectionString = "FullUri=https://example.test/app.db";

        var exception = Assert.Throws<PSArgumentException>(() => DbaXProviderHelpers.GetSQLiteDatabasePath(connectionString, "SQLite maintenance"));

        Assert.Contains("file-backed SQLite FullUri", exception.Message);
    }

    [Fact]
    public void GetSQLiteDatabasePath_RejectsDataSourceUriMemoryConnectionStrings()
    {
        var path = Path.Join(Path.GetTempPath(), "dbaclientx-datasource-uri-memory-path.db");
        var connectionString = "Data Source=" + new Uri(path).AbsoluteUri + "?mode=memory&cache=shared";

        var exception = Assert.Throws<PSArgumentException>(() => DbaXProviderHelpers.GetSQLiteDatabasePath(connectionString, "SQLite maintenance"));

        Assert.Contains("file-backed", exception.Message);
    }

    [Theory]
    [InlineData(@"..\unsafe.db")]
    [InlineData(@"Data Source=..\unsafe.db")]
    public void GetSQLiteDatabasePath_RejectsUnsafeRelativePaths(string input)
    {
        var exception = Assert.Throws<PSArgumentException>(() => DbaXProviderHelpers.GetSQLiteDatabasePath(input, "SQLite maintenance"));

        Assert.Contains("unsafe relative path", exception.Message);
    }

    [Theory]
    [InlineData(@"..\unsafe.db")]
    [InlineData(@"Data Source=..\unsafe.db")]
    public void GetSQLiteReadOnlyConnectionString_RejectsUnsafeRelativePaths(string input)
    {
        var exception = Assert.Throws<PSArgumentException>(() => DbaXProviderHelpers.GetSQLiteReadOnlyConnectionString(input));

        Assert.Contains("unsafe relative path", exception.Message);
    }

    [Fact]
    public void GetSQLiteDatabasePath_RejectsConnectionStringsWithoutDatabase()
    {
        const string connectionString = "Mode=ReadOnly";

        var exception = Assert.Throws<PSArgumentException>(() => DbaXProviderHelpers.GetSQLiteDatabasePath(connectionString, "SQLite maintenance"));

        Assert.Contains("Data Source", exception.Message);
    }

    [Fact]
    public void GetCapabilities_ReportsStreamingOnlyWhenLoadedTargetSupportsIt()
    {
        var capabilities = DbaXProviderHelpers.GetCapabilities(DbaXProvider.SqlServer);

        Assert.Equal(DbaXProviderHelpers.SupportsStreaming, capabilities.HasFlag(DbaXProviderCapability.Streaming));
    }

    [Theory]
    [InlineData(":memory:", false)]
    [InlineData("Data Source=:memory:", false)]
    [InlineData("Data Source=shared;Mode=Memory;Cache=Shared", false)]
    [InlineData(@"C:\data\app.db", true)]
    [InlineData(@"Data Source=C:\data\app.db", true)]
    [InlineData(@"Data Source=C:\data\app.db;Mode=ReadOnly", true)]
    public void IsSQLiteFileBackedDatabase_DetectsFileBackedTargets(string input, bool expected)
    {
        var actual = DbaXProviderHelpers.IsSQLiteFileBackedDatabase(input);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void ExecutePing_SQLiteMissingFile_DoesNotCreateDatabase()
    {
        var path = Path.Join(Path.GetTempPath(), "dbaclientx-missing-" + Guid.NewGuid().ToString("N") + ".db");

        var exception = Assert.Throws<InvalidOperationException>(() => DbaXProviderHelpers.ExecutePing(DbaXProvider.SQLite, path));

        Assert.Contains("does not exist", exception.Message);
        Assert.False(File.Exists(path));
    }

    [Fact]
    public void ExecutePing_SQLiteMemoryConnectionString_DoesNotTreatDataSourceAsFile()
    {
        var database = "dbaclientx-memory-" + Guid.NewGuid().ToString("N");

        var result = DbaXProviderHelpers.ExecutePing(DbaXProvider.SQLite, $"Data Source={database};Mode=Memory;Cache=Shared");

        Assert.Equal(1L, result);
        Assert.False(File.Exists(database));
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
