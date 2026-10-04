using System.Data;
using DBAClientX;
using Microsoft.Data.SqlClient;

namespace DbaClientX.Tests;

public sealed class SqlServerIntegrityNativeTests
{
    private readonly ITestOutputHelper _output;
    public SqlServerIntegrityNativeTests(ITestOutputHelper output) => _output = output;

    [Fact]
    [Trait("Category", "LiveSqlRecovery")]
    public async Task DamagedOwnedRestoreReportsBoundedIntegrityIssuesWithoutMessagesByDefault()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQL_BACKUP_TEST_CONNECTION");
        string? directory = Environment.GetEnvironmentVariable("DBACLIENTX_SQL_BACKUP_TEST_DIRECTORY");
        Assert.SkipWhen(!OperatingSystem.IsWindows()
            || Environment.GetEnvironmentVariable("DBACLIENTX_SQL_CORRUPTION_TEST") != "1"
            || string.IsNullOrWhiteSpace(connectionString) || string.IsNullOrWhiteSpace(directory),
            "Explicit local Windows page-damage qualification requires DBACLIENTX_SQL_CORRUPTION_TEST=1 and the SQL recovery lab settings.");
        var builder = new SqlConnectionStringBuilder(connectionString)
        { InitialCatalog = "master", Pooling = false, Enlist = false };
        Assert.Equal("localhost", builder.DataSource);
        await using var scope = new SqlServerRecoveryTestScope(builder.ConnectionString, directory!);
        await using var observer = new SqlConnection(builder.ConnectionString);
        await observer.OpenAsync();
        await scope.CreateSourceAsync(observer);
        using (var fill = new SqlCommand($"ALTER DATABASE [{scope.SourceName}] SET PAGE_VERIFY CHECKSUM; "
            + $"CREATE TABLE [{scope.SourceName}].dbo.IntegrityProbe(Value int NOT NULL, Payload varbinary(8000) NOT NULL); "
            + $"INSERT INTO [{scope.SourceName}].dbo.IntegrityProbe VALUES(42, CONVERT(varbinary(8000),REPLICATE('A',8000)))", observer))
            await fill.ExecuteNonQueryAsync();
        using var provider = new SqlServer();
        var backup = await provider.BackupDatabaseCopyOnlyToDiskAsync(builder.ConnectionString, scope.SourceName, scope.BackupDirectory);
        var files = await provider.ReadDiskBackupFileListAsync(builder.ConnectionString, backup.ServerBackupPath);
        var destinations = files.Select((file, i) => (file.LogicalName, Path: Path.Combine(scope.BackupDirectory,
            "target" + i + (file.FileType == "L" ? ".ldf" : ".mdf"))))
            .ToDictionary(file => file.LogicalName, file => file.Path);
        scope.RecordRestoreFiles(destinations.Values);
        var plan = await provider.PrepareRestoreAsNewAsync(builder.ConnectionString, backup.ServerBackupPath,
            scope.RestoreName, backup.Header.Identity, destinations);
        await provider.RestoreDatabaseAsNewAsync(builder.ConnectionString, plan);
        Assert.True((await provider.CheckDatabaseIntegrityAsync(builder.ConnectionString, scope.RestoreName)).Succeeded);
        Assert.Equal(42, Convert.ToInt32(await provider.ExecuteScalarAsync(builder.ConnectionString,
            $"SELECT Value FROM [{scope.RestoreName}].dbo.IntegrityProbe")));

        int fileId, pageId;
        using (var page = new SqlCommand("SELECT TOP(1) allocated_page_file_id,allocated_page_page_id "
            + "FROM sys.dm_db_database_page_allocations(DB_ID(@database),OBJECT_ID(@object),NULL,NULL,'DETAILED') "
            + "WHERE is_allocated=1 AND page_type=1", observer))
        {
            page.Parameters.Add("@database", SqlDbType.NVarChar, 128).Value = scope.RestoreName;
            page.Parameters.Add("@object", SqlDbType.NVarChar, 300).Value = scope.RestoreName + ".dbo.IntegrityProbe";
            using var reader = await page.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync(), "No owned heap data page found.");
            fileId = Convert.ToInt32(reader.GetValue(0));
            pageId = Convert.ToInt32(reader.GetValue(1));
        }
        string path = destinations[files.Single(file => file.FileId == fileId).LogicalName];
        Assert.True(pageId > 8);
        Assert.Equal(Path.GetFullPath(scope.BackupDirectory), Path.GetDirectoryName(Path.GetFullPath(path)));
        Assert.Equal((FileAttributes)0, File.GetAttributes(path) & FileAttributes.ReparsePoint);
        using (var single = new SqlCommand($"ALTER DATABASE [{scope.RestoreName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE", observer))
            await single.ExecuteNonQueryAsync();
        try
        {
            using (var ownership = new SqlCommand("SELECT d.state_desc, mf.physical_name FROM sys.databases d "
                + "JOIN sys.master_files mf ON d.database_id=mf.database_id WHERE d.name=@name AND mf.file_id=@file", observer))
            {
                ownership.Parameters.Add("@name", SqlDbType.NVarChar, 128).Value = scope.RestoreName;
                ownership.Parameters.Add("@file", SqlDbType.Int).Value = fileId;
                using var reader = await ownership.ExecuteReaderAsync();
                Assert.True(await reader.ReadAsync());
                Assert.Equal("ONLINE", reader.GetString(0));
                Assert.Equal(Path.GetFullPath(path), Path.GetFullPath(reader.GetString(1)), StringComparer.OrdinalIgnoreCase);
            }
            // Test-only native direct-write mode flushes this database and bypasses checksum
            // recalculation. The database/name/files were absent before this scope's restore;
            // the page belongs to its one-row heap. Offset 512 lies within the fixed 'A' payload.
            // Never expose this unsupported diagnostic command through a production API.
            using var damage = new SqlCommand($"DBCC WRITEPAGE (N'{scope.RestoreName}', {fileId}, {pageId}, 512, 1, 0x42, 1)", observer);
            await damage.ExecuteNonQueryAsync();
        }
        finally
        {
            using var multi = new SqlCommand($"ALTER DATABASE [{scope.RestoreName}] SET MULTI_USER WITH ROLLBACK IMMEDIATE", observer);
            await multi.ExecuteNonQueryAsync();
        }
        _output.WriteLine($"Damaged owned user data page {fileId}:{pageId}; restored database was healthy before damage.");

        var bounded = await provider.CheckDatabaseIntegrityAsync(builder.ConnectionString, scope.RestoreName,
            new SqlServerIntegrityCheckOptions { MaxIssues = 1 });
        Assert.False(bounded.Succeeded);
        Assert.False(bounded.PhysicalOnly);
        Assert.True(bounded.IssueCount > 1);
        Assert.Single(bounded.Issues);
        Assert.True(bounded.IssuesTruncated);
        Assert.Null(bounded.Issues[0].Message);
        Assert.True(bounded.Issues[0].ErrorNumber > 0);
        Assert.True(bounded.Issues[0].Severity > 0);
        var detailed = await provider.CheckDatabaseIntegrityAsync(builder.ConnectionString, scope.RestoreName,
            new SqlServerIntegrityCheckOptions { IncludeDiagnosticMessages = true, MaxIssues = 1000 });
        Assert.False(detailed.Succeeded);
        Assert.Equal(bounded.IssueCount, detailed.IssueCount);
        Assert.False(detailed.IssuesTruncated);
        Assert.All(detailed.Issues, issue => Assert.False(string.IsNullOrWhiteSpace(issue.Message)));
        var physical = await provider.CheckDatabaseIntegrityAsync(builder.ConnectionString, scope.RestoreName,
            new SqlServerIntegrityCheckOptions { PhysicalOnly = true, MaxIssues = 1 });
        Assert.True(physical.PhysicalOnly);
        Assert.False(physical.Succeeded);
        _output.WriteLine($"Native full CHECKDB issues={bounded.IssueCount}; bounded/message-opt-in/physical-only contracts passed.");
    }
}
