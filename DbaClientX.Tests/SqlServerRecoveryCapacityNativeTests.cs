using System.Diagnostics;
using DBAClientX;
using Microsoft.Data.SqlClient;

namespace DbaClientX.Tests;

[Collection(SqlServerRecoveryCapacityCollection.Name)]
public sealed class SqlServerRecoveryCapacityNativeTests
{
    private readonly ITestOutputHelper _output;
    public SqlServerRecoveryCapacityNativeTests(ITestOutputHelper output) => _output = output;

    [Fact]
    [Trait("Category", "LiveSqlRecovery")]
    public async Task TenGiBAllocatedRestorePreservesFileCapacityAndIndependentApplicationWrites()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQL_BACKUP_TEST_CONNECTION");
        string? directory = Environment.GetEnvironmentVariable("DBACLIENTX_SQL_BACKUP_TEST_DIRECTORY");
        Assert.SkipWhen(!OperatingSystem.IsWindows()
            || Environment.GetEnvironmentVariable("DBACLIENTX_SQL_LARGE_RECOVERY_TEST") != "1"
            || string.IsNullOrWhiteSpace(connectionString) || string.IsNullOrWhiteSpace(directory),
            "Explicit 10 GiB allocation qualification requires DBACLIENTX_SQL_LARGE_RECOVERY_TEST=1 and the local SQL recovery lab settings.");
        var builder = new SqlConnectionStringBuilder(connectionString)
        { InitialCatalog = "master", Pooling = false, Enlist = false };
        Assert.Equal("localhost", builder.DataSource);
        // Fixture capacity guard for this local-only probe, not a server-capacity product API.
        // Source and restored 10 GiB files coexist, with another 12 GiB of local headroom.
        var volume = new DriveInfo(Path.GetPathRoot(Path.GetFullPath(directory!))!);
        Assert.True(volume.IsReady && volume.AvailableFreeSpace >= 32L * 1024 * 1024 * 1024);
        await using var scope = new SqlServerRecoveryTestScope(builder.ConnectionString, directory!);
        await using var observer = new SqlConnection(builder.ConnectionString);
        await observer.OpenAsync();
        await scope.CreateSourceAsync(observer);
        string sourcePath = Path.Combine(scope.BackupDirectory, "allocated.ndf");
        scope.RecordAdditionalSourceFile(sourcePath);
        using (var fill = new SqlCommand($"ALTER DATABASE [{scope.SourceName}] ADD FILE "
            + $"(NAME=N'CapacityProbe',FILENAME=N'{sourcePath.Replace("'", "''")}',SIZE=10240MB,FILEGROWTH=0); "
            + $"CREATE TABLE [{scope.SourceName}].dbo.ApplicationProbe(Value int NOT NULL); "
            + $"INSERT INTO [{scope.SourceName}].dbo.ApplicationProbe VALUES(42)", observer)
            { CommandTimeout = 300 })
            await fill.ExecuteNonQueryAsync();
        using var provider = new SqlServer();
        var backup = await provider.BackupDatabaseCopyOnlyToDiskAsync(builder.ConnectionString,
            scope.SourceName, scope.BackupDirectory);
        var files = await provider.ReadDiskBackupFileListAsync(builder.ConnectionString, backup.ServerBackupPath);
        var allocated = Assert.Single(files, file => file.LogicalName == "CapacityProbe");
        Assert.Equal(10L * 1024 * 1024 * 1024, allocated.SizeBytes);
        var destinations = files.Select((file, i) => (file.LogicalName, Path: Path.Combine(scope.BackupDirectory,
            "target" + i + (file.FileType == "L" ? ".ldf" : ".mdf"))))
            .ToDictionary(file => file.LogicalName, file => file.Path);
        scope.RecordRestoreFiles(destinations.Values);
        var plan = await provider.PrepareRestoreAsNewAsync(builder.ConnectionString, backup.ServerBackupPath,
            scope.RestoreName, backup.Header.Identity, destinations);
        Assert.Equal(files.Sum(file => file.SizeBytes), plan.RequiredFileBytes);
        Assert.True(plan.RequiredFileBytes >= allocated.SizeBytes);
        Assert.False(await SqlServerRecoveryTestScope.DatabaseExistsAsync(observer, scope.RestoreName));
        Assert.All(destinations.Values, path => Assert.False(File.Exists(path)));
        var timer = Stopwatch.StartNew();
        await provider.RestoreDatabaseAsNewAsync(builder.ConnectionString, plan);
        timer.Stop();
        using (var sizes = new SqlCommand($"SELECT SUM(CONVERT(bigint,size)*8192) "
            + $"FROM [{scope.RestoreName}].sys.database_files", observer))
            Assert.Equal(plan.RequiredFileBytes, Convert.ToInt64(await sizes.ExecuteScalarAsync()));
        Assert.True((await provider.CheckDatabaseIntegrityAsync(builder.ConnectionString, scope.RestoreName)).Succeeded);
        Assert.Equal(42, Convert.ToInt32(await provider.ExecuteScalarAsync(builder.ConnectionString,
            $"SELECT Value FROM [{scope.RestoreName}].dbo.ApplicationProbe")));
        await provider.BeginTransactionAsync(builder.ConnectionString);
        Assert.Equal(1, await provider.ExecuteNonQueryAsync(builder.ConnectionString,
            $"UPDATE [{scope.RestoreName}].dbo.ApplicationProbe SET Value=43", useTransaction: true));
        Assert.Equal(43, Convert.ToInt32(await provider.ExecuteScalarAsync(builder.ConnectionString,
            $"SELECT Value FROM [{scope.RestoreName}].dbo.ApplicationProbe", useTransaction: true)));
        await provider.CommitAsync();
        Assert.Equal(43, Convert.ToInt32(await provider.ExecuteScalarAsync(builder.ConnectionString,
            $"SELECT Value FROM [{scope.RestoreName}].dbo.ApplicationProbe")));
        Assert.Equal(42, Convert.ToInt32(await provider.ExecuteScalarAsync(builder.ConnectionString,
            $"SELECT Value FROM [{scope.SourceName}].dbo.ApplicationProbe")));
        _output.WriteLine($"Allocated restore bytes={plan.RequiredFileBytes}; restore elapsed={timer.Elapsed}; full CHECKDB and independent committed application read/write passed. This mostly empty allocation is not populated-data throughput proof.");
    }
}
