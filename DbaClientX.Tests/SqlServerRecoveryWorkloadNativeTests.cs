using System.Data;
using System.Diagnostics;
using DBAClientX;
using DBAClientX.Diagnostics;
using Microsoft.Data.SqlClient;

namespace DbaClientX.Tests;

public sealed class SqlServerRecoveryWorkloadNativeTests
{
    private const int PopulatedRows = 1_350_000;
    private readonly ITestOutputHelper _output;
    public SqlServerRecoveryWorkloadNativeTests(ITestOutputHelper output) => _output = output;

    [Fact]
    [Trait("Category", "LiveSqlRecovery")]
    public async Task ChainRetainsGrowthAfterFullBackupAndRestoresLastCommittedValue()
    {
        var (connectionString, directory) = Configuration();
        await using var scope = new SqlServerRecoveryTestScope(connectionString, directory);
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await scope.CreateSourceAsync(connection);
        await AddDataFileAsync(connection, scope, "Growth", 16, 256);
        await ExecuteAsync(connection, $"ALTER DATABASE [{scope.SourceName}] SET RECOVERY FULL; "
            + $"CREATE TABLE [{scope.SourceName}].dbo.ApplicationProbe(Value int NOT NULL); "
            + $"INSERT INTO [{scope.SourceName}].dbo.ApplicationProbe VALUES(1)");
        using var provider = RecoveryProvider();
        var full = await provider.BackupToDedicatedDiskAsync(connectionString, scope.SourceName,
            scope.BackupDirectory, SqlServerDiskBackupKind.Full, copyOnly: false);
        var initialFiles = await provider.ReadDiskBackupFileListAsync(connectionString, full.ServerBackupPath);
        await ExecuteAsync(connection, $"ALTER DATABASE [{scope.SourceName}] MODIFY FILE(NAME=N'Growth',SIZE=256MB); "
            + $"UPDATE [{scope.SourceName}].dbo.ApplicationProbe SET Value=2");
        var differential = await provider.BackupToDedicatedDiskAsync(connectionString, scope.SourceName,
            scope.BackupDirectory, SqlServerDiskBackupKind.Differential, copyOnly: false);
        await ExecuteAsync(connection, $"ALTER DATABASE [{scope.SourceName}] MODIFY FILE(NAME=N'{scope.SourceName}_log',SIZE=128MB); "
            + $"UPDATE [{scope.SourceName}].dbo.ApplicationProbe SET Value=3");
        var log = await provider.BackupToDedicatedDiskAsync(connectionString, scope.SourceName,
            scope.BackupDirectory, SqlServerDiskBackupKind.Log, copyOnly: false);
        var finalFiles = await provider.ReadDiskBackupFileListAsync(connectionString, log.ServerBackupPath);
        var destinations = Destinations(scope, initialFiles);
        var sources = new[] { full, differential, log }.Select(backup =>
            new SqlServerRestoreChainSource(backup.ServerBackupPath, backup.Header.Identity)).ToArray();
        var plan = await provider.PrepareRestoreChainAsNewAsync(connectionString, sources, scope.RestoreName, destinations);
        Assert.Equal(256L * 1024 * 1024, plan.RequiredFiles.Single(file => file.LogicalName == "Growth").SizeBytes);
        Assert.Equal(128L * 1024 * 1024, plan.RequiredFiles.Single(file => file.FileType == "L").SizeBytes);
        Assert.Equal(finalFiles.Sum(file => file.SizeBytes), plan.RequiredFileBytes);
        Assert.True(plan.RequiredFileBytes > initialFiles.Sum(file => file.SizeBytes));
        Assert.False(await SqlServerRecoveryTestScope.DatabaseExistsAsync(connection, scope.RestoreName));
        Assert.All(destinations.Values, path => Assert.False(File.Exists(path)));
        await ExecuteAsync(connection, $"UPDATE [{scope.SourceName}].dbo.ApplicationProbe SET Value=4");
        using var telemetry = DbaClientXDiagnostics.StartOperation("recovery-grown-chain", null);
        await provider.RestoreChainAsNewAsync(connectionString, plan);
        Assert.Equal(plan.RequiredFileBytes, await ScalarAsync<long>(connection,
            $"SELECT SUM(CONVERT(bigint,size)*8192) FROM [{scope.RestoreName}].sys.database_files"));
        Assert.Equal(3, await ScalarAsync<int>(connection, $"SELECT Value FROM [{scope.RestoreName}].dbo.ApplicationProbe"));
        Assert.Equal(4, await ScalarAsync<int>(connection, $"SELECT Value FROM [{scope.SourceName}].dbo.ApplicationProbe"));
        Assert.True((await provider.CheckDatabaseIntegrityAsync(connectionString, scope.RestoreName)).Succeeded);
        Assert.Equal(0, telemetry.Telemetry.RetryCount);
        _output.WriteLine($"Full allocation={initialFiles.Sum(file => file.SizeBytes)}; chain maximum={plan.RequiredFileBytes}; restored allocation and committed value3 match the pinned chain.");
    }

    [Fact]
    [Trait("Category", "LiveSqlRecovery")]
    public async Task ConfiguredDataFileLimitFailsWithoutReplayAndRollbackRetainsEarlierCommit()
    {
        var (connectionString, directory) = Configuration();
        await using var scope = new SqlServerRecoveryTestScope(connectionString, directory);
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await scope.CreateSourceAsync(connection);
        await AddDataFileAsync(connection, scope, "Limited", 8, 16);
        await ExecuteAsync(connection, $"ALTER DATABASE [{scope.SourceName}] SET RECOVERY SIMPLE; "
            + $"CREATE TABLE [{scope.SourceName}].dbo.LimitedRows(Id int NOT NULL,Payload binary(8000) NOT NULL) ON [Limited]; "
            + $"INSERT INTO [{scope.SourceName}].dbo.LimitedRows VALUES(0,CONVERT(binary(8000),0x2A))");
        string application = ApplicationConnection(connectionString, scope.SourceName);
        using var provider = RecoveryProvider();
        using var telemetry = DbaClientXDiagnostics.StartOperation("recovery-file-limit", null);
        await provider.BeginTransactionAsync(application);
        try
        {
            var failure = await Assert.ThrowsAsync<DbaQueryExecutionException>(() => provider.ExecuteNonQueryAsync(application,
                $"INSERT INTO dbo.LimitedRows WITH (TABLOCK) SELECT TOP(10000) ROW_NUMBER() OVER(ORDER BY (SELECT NULL)),CONVERT(binary(8000),0xFF) "
                + "FROM sys.all_objects a CROSS JOIN sys.all_objects b", useTransaction: true));
            // Page and object allocation report different native codes at the same file limit.
            Assert.Contains(failure.ProviderErrorCode, new int?[] { 1101, 1105 });
        }
        finally { await provider.RollbackAsync(); }
        Assert.Equal(0, telemetry.Telemetry.RetryCount);
        Assert.Equal(1L, await ScalarAsync<long>(connection, $"SELECT COUNT_BIG(*) FROM [{scope.SourceName}].dbo.LimitedRows"));
        Assert.Equal(1L, await ScalarAsync<long>(connection,
            $"SELECT COUNT_BIG(*) FROM [{scope.SourceName}].dbo.LimitedRows WHERE Id=0 AND Payload=CONVERT(binary(8000),0x2A)"));
        Assert.InRange(await ScalarAsync<long>(connection,
            $"SELECT CONVERT(bigint,size)*8192 FROM [{scope.SourceName}].sys.database_files WHERE name=N'Limited'"), 8L * 1024 * 1024, 16L * 1024 * 1024);
        Assert.Equal(1, await provider.ExecuteNonQueryAsync(application, "INSERT INTO dbo.LimitedRows VALUES(1,CONVERT(binary(8000),0x01))"));
        Assert.Equal(2L, await ScalarAsync<long>(connection, $"SELECT COUNT_BIG(*) FROM [{scope.SourceName}].dbo.LimitedRows"));
        Assert.True((await provider.CheckDatabaseIntegrityAsync(connectionString, scope.SourceName)).Succeeded);
        _output.WriteLine("Native page/object allocation failure at the unique16MiB file limit: no replay, earlier commit retained, failed transaction rolled back, subsequent write and fullCHECKDB pass. This is not volume-exhaustion proof.");
    }

    [Fact]
    [Trait("Category", "LiveSqlRecovery")]
    public async Task PopulatedRecoverySupportsConcurrentStreamBulkAndCommittedApplicationWrites()
    {
        var (connectionString, directory) = Configuration(populated: true);
        await using var scope = new SqlServerRecoveryTestScope(connectionString, directory);
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await scope.CreateSourceAsync(connection);
        await AddDataFileAsync(connection, scope, "Populated", 12288, 16384);
        await ExecuteAsync(connection, $"ALTER DATABASE [{scope.SourceName}] SET RECOVERY SIMPLE; "
            + $"CREATE TABLE [{scope.SourceName}].dbo.PayloadRows(Id int NOT NULL PRIMARY KEY NONCLUSTERED,Payload binary(8000) NOT NULL) ON [Populated]; "
            + $"CREATE TABLE [{scope.SourceName}].dbo.ApplicationProbe(Value int NOT NULL); INSERT INTO [{scope.SourceName}].dbo.ApplicationProbe VALUES(0); "
            + $"CREATE TABLE [{scope.SourceName}].dbo.QueueRows(Id int NOT NULL PRIMARY KEY,Value int NOT NULL)");
        // Bounded server-side batches avoid retaining the populated dataset in the test process.
        for (int start = 1; start <= PopulatedRows; start += 10000)
        {
            int count = Math.Min(10000, PopulatedRows - start + 1);
            await ExecuteAsync(connection, $"INSERT INTO [{scope.SourceName}].dbo.PayloadRows WITH (TABLOCK) "
                + $"SELECT n,CONVERT(binary(8000),CONVERT(binary(4),n)) FROM (SELECT TOP({count}) "
                + $"{start}-1+CONVERT(int,ROW_NUMBER() OVER(ORDER BY (SELECT NULL))) n FROM sys.all_objects a CROSS JOIN sys.all_objects b) numbers");
        }
        var expected = await FingerprintAsync(connection, scope.SourceName);
        Assert.Equal(PopulatedRows, expected.Rows);
        Assert.True(expected.PayloadBytes >= 10L * 1024 * 1024 * 1024);
        Assert.Equal((long)PopulatedRows * (PopulatedRows + 1) / 2, expected.KeySum);
        Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT COUNT_BIG(*) FROM [{scope.SourceName}].dbo.PayloadRows "
            + "WHERE Payload<>CONVERT(binary(8000),CONVERT(binary(4),Id))"));
        using var provider = RecoveryProvider();
        using var telemetry = DbaClientXDiagnostics.StartOperation("recovery-populated-workload", null);
        var timer = Stopwatch.StartNew();
        var backup = await provider.BackupDatabaseCopyOnlyToDiskAsync(connectionString, scope.SourceName, scope.BackupDirectory);
        var files = await provider.ReadDiskBackupFileListAsync(connectionString, backup.ServerBackupPath);
        var plan = await provider.PrepareRestoreAsNewAsync(connectionString, backup.ServerBackupPath,
            scope.RestoreName, backup.Header.Identity, Destinations(scope, files));
        await provider.RestoreDatabaseAsNewAsync(connectionString, plan);
        Assert.Equal(expected, await FingerprintAsync(connection, scope.RestoreName));
        Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT COUNT_BIG(*) FROM [{scope.RestoreName}].dbo.PayloadRows "
            + "WHERE Payload<>CONVERT(binary(8000),CONVERT(binary(4),Id))"));
        Assert.True((await provider.CheckDatabaseIntegrityAsync(connectionString, scope.RestoreName)).Succeeded);
        await RunMixedApplicationAsync(connectionString, scope.RestoreName);
        Assert.Equal(expected, await FingerprintAsync(connection, scope.RestoreName));
        Assert.Equal(64, await ScalarAsync<int>(connection, $"SELECT Value FROM [{scope.RestoreName}].dbo.ApplicationProbe"));
        Assert.Equal(0, await ScalarAsync<int>(connection, $"SELECT Value FROM [{scope.SourceName}].dbo.ApplicationProbe"));
        Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT COUNT_BIG(*) FROM [{scope.SourceName}].dbo.QueueRows"));
        Assert.True((await provider.CheckDatabaseIntegrityAsync(connectionString, scope.RestoreName)).Succeeded);
        Assert.Equal(0, telemetry.Telemetry.RetryCount);
        _output.WriteLine($"Populated payload bytes={expected.PayloadBytes}; rows={expected.Rows}; recorded restore allocation={plan.RequiredFileBytes}; "
            + $"recovery/application qualification elapsed={timer.Elapsed}. Full payload comparison/CHECKDB, streamed row/key totals, bulk10000 and64 committed writes pass. No throughput ranking or insufficient-volume-space claim.");
    }

    private static async Task RunMixedApplicationAsync(string connectionString, string database)
    {
        string application = ApplicationConnection(connectionString, database);
        using var readerProvider = RecoveryProvider();
        using var writerProvider = RecoveryProvider();
        using var bulkProvider = RecoveryProvider();
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        var readerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var writesStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task stream = ReadAsync();
        Task writes = WriteAsync();
        Task bulk = BulkAsync();
        try { await Task.WhenAll(stream, writes, bulk); }
        finally { timeout.Cancel(); }
        Assert.Equal(10000L, Convert.ToInt64(await bulkProvider.ExecuteScalarAsync(application, "SELECT COUNT_BIG(*) FROM dbo.QueueRows")));
        Assert.Equal(50005000L, Convert.ToInt64(await bulkProvider.ExecuteScalarAsync(application, "SELECT SUM(CONVERT(bigint,Id)) FROM dbo.QueueRows")));
        Assert.Equal(0L, Convert.ToInt64(await bulkProvider.ExecuteScalarAsync(application, "SELECT COUNT_BIG(*) FROM dbo.QueueRows WHERE Value<>Id*2")));

        async Task ReadAsync()
        {
            long rows = 0, sum = 0;
            try
            {
                await foreach (int id in readerProvider.QueryStreamAsync(application,
                    "SELECT Id FROM dbo.PayloadRows ORDER BY Id", record => record.GetInt32(0), cancellationToken: timeout.Token))
                {
                    Assert.Equal(++rows, id);
                    sum += id;
                    if (rows == 1)
                    {
                        readerStarted.TrySetResult();
                        await writesStarted.Task.WaitAsync(timeout.Token);
                    }
                }
                Assert.Equal(PopulatedRows, rows);
                Assert.Equal((long)PopulatedRows * (PopulatedRows + 1) / 2, sum);
            }
            catch { timeout.Cancel(); throw; }
        }

        async Task WriteAsync()
        {
            try
            {
                await readerStarted.Task.WaitAsync(timeout.Token);
                for (int value = 1; value <= 64; value++)
                {
                    await writerProvider.BeginTransactionAsync(application, cancellationToken: timeout.Token);
                    try
                    {
                        Assert.Equal(1, await writerProvider.ExecuteNonQueryAsync(application,
                            "UPDATE dbo.ApplicationProbe SET Value=@value", new Dictionary<string, object?> { ["value"] = value },
                            useTransaction: true, cancellationToken: timeout.Token));
                        await writerProvider.CommitAsync(timeout.Token);
                    }
                    catch { await writerProvider.RollbackAsync(); throw; }
                    if (value == 1) writesStarted.TrySetResult();
                }
            }
            catch { timeout.Cancel(); throw; }
        }

        async Task BulkAsync()
        {
            try
            {
                await readerStarted.Task.WaitAsync(timeout.Token);
                await using var input = new SqlConnection(connectionString);
                await input.OpenAsync(timeout.Token);
                using var command = new SqlCommand("SELECT n AS Id,n*2 AS Value FROM (SELECT TOP(10000) "
                    + "CONVERT(int,ROW_NUMBER() OVER(ORDER BY (SELECT NULL))) n FROM sys.all_objects a CROSS JOIN sys.all_objects b) numbers", input);
                await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, timeout.Token);
                await bulkProvider.BeginTransactionAsync(application, cancellationToken: timeout.Token);
                try
                {
                    await bulkProvider.BulkInsertAsync(application, reader, "dbo.QueueRows", useTransaction: true,
                        batchSize: 1024, bulkCopyTimeout: 120, cancellationToken: timeout.Token);
                    await bulkProvider.CommitAsync(timeout.Token);
                }
                catch { await bulkProvider.RollbackAsync(); throw; }
            }
            catch { timeout.Cancel(); throw; }
        }
    }

    private static (string Connection, string Directory) Configuration(bool populated = false)
    {
        string? connection = Environment.GetEnvironmentVariable("DBACLIENTX_SQL_BACKUP_TEST_CONNECTION");
        string? directory = Environment.GetEnvironmentVariable("DBACLIENTX_SQL_BACKUP_TEST_DIRECTORY");
        Assert.SkipWhen(!OperatingSystem.IsWindows() || string.IsNullOrWhiteSpace(connection) || string.IsNullOrWhiteSpace(directory)
            || Environment.GetEnvironmentVariable("DBACLIENTX_SQL_WORKLOAD_TEST") != "1"
            || (populated && Environment.GetEnvironmentVariable("DBACLIENTX_SQL_POPULATED_RECOVERY_TEST") != "1"),
            "Local recovery workload qualification requires DBACLIENTX_SQL_WORKLOAD_TEST=1 and recovery lab settings; the populated10GiB probe also requires DBACLIENTX_SQL_POPULATED_RECOVERY_TEST=1.");
        var builder = new SqlConnectionStringBuilder(connection) { InitialCatalog = "master", Pooling = false, Enlist = false };
        Assert.Equal("localhost", builder.DataSource);
        var volume = new DriveInfo(Path.GetPathRoot(Path.GetFullPath(directory!))!);
        Assert.True(volume.IsReady && volume.AvailableFreeSpace >= (populated ? 48L : 2L) * 1024 * 1024 * 1024);
        return (builder.ConnectionString, directory!);
    }

    private static SqlServer RecoveryProvider() => new() { CommandRetryMode = CommandRetryMode.ReplaySafe, MaxRetryAttempts = 3 };
    private static string ApplicationConnection(string connectionString, string database)
        => new SqlConnectionStringBuilder(connectionString) { InitialCatalog = database }.ConnectionString;

    private static async Task AddDataFileAsync(SqlConnection connection, SqlServerRecoveryTestScope scope, string logical, int initialMiB, int maximumMiB)
    {
        string path = Path.Combine(scope.BackupDirectory, logical + ".ndf");
        scope.RecordAdditionalSourceFile(path);
        await ExecuteAsync(connection, $"ALTER DATABASE [{scope.SourceName}] ADD FILEGROUP [{logical}]; "
            + $"ALTER DATABASE [{scope.SourceName}] ADD FILE(NAME=N'{logical}',FILENAME=N'{path.Replace("'", "''")}',"
            + $"SIZE={initialMiB}MB,MAXSIZE={maximumMiB}MB,FILEGROWTH=8MB) TO FILEGROUP [{logical}]");
    }

    private static Dictionary<string, string> Destinations(SqlServerRecoveryTestScope scope, IReadOnlyList<SqlServerBackupFileInfo> files)
    {
        var destinations = files.Select((file, index) => (file.LogicalName,
            Path: Path.Combine(scope.BackupDirectory, "target" + index + (file.FileType == "L" ? ".ldf" : ".mdf"))))
            .ToDictionary(item => item.LogicalName, item => item.Path);
        scope.RecordRestoreFiles(destinations.Values);
        return destinations;
    }

    private static async Task ExecuteAsync(SqlConnection connection, string sql)
    { using var command = new SqlCommand(sql, connection) { CommandTimeout = 300 }; await command.ExecuteNonQueryAsync(); }
    private static async Task<T> ScalarAsync<T>(SqlConnection connection, string sql)
    { using var command = new SqlCommand(sql, connection) { CommandTimeout = 300 }; return (T)Convert.ChangeType((await command.ExecuteScalarAsync())!, typeof(T)); }
    private static async Task<PayloadFingerprint> FingerprintAsync(SqlConnection connection, string database)
    {
        using var command = new SqlCommand($"SELECT COUNT_BIG(*),SUM(CONVERT(bigint,Id)),SUM(CONVERT(bigint,DATALENGTH(Payload))) "
            + $"FROM [{database}].dbo.PayloadRows", connection) { CommandTimeout = 300 };
        using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        return new(reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2));
    }
    private sealed record PayloadFingerprint(long Rows, long KeySum, long PayloadBytes);
}
