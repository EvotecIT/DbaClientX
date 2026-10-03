using System.Data;
using System.Diagnostics;
using System.IO;
using DBAClientX;
using DBAClientX.Diagnostics;
using Microsoft.Data.SqlClient;

namespace DbaClientX.Tests;

public sealed class SqlServerRestoreChainNativeTests
{
    [Fact]
    [Trait("Category", "LiveSqlRecovery")]
    public async Task FullDifferentialLogChainRestoresAndRejectsGapsBeforeCreation()
    {
        var (connectionString, directory) = Configuration();
        await using var scope = new SqlServerRecoveryTestScope(connectionString, directory);
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await scope.CreateSourceAsync(connection);
        await Execute(connection, $"ALTER DATABASE [{scope.SourceName}] SET RECOVERY FULL; CREATE TABLE [{scope.SourceName}].dbo.ChainProbe(Value int NOT NULL); INSERT INTO [{scope.SourceName}].dbo.ChainProbe VALUES(1)");
        using var provider = new ReplayTrackingProvider();
        var full = await provider.BackupToDedicatedDiskAsync(connectionString, scope.SourceName, scope.BackupDirectory, SqlServerDiskBackupKind.Full, false);
        var copy = await provider.BackupDatabaseCopyOnlyToDiskAsync(connectionString, scope.SourceName, scope.BackupDirectory);
        await Execute(connection, $"UPDATE [{scope.SourceName}].dbo.ChainProbe SET Value=2");
        var differential = await provider.BackupToDedicatedDiskAsync(connectionString, scope.SourceName, scope.BackupDirectory, SqlServerDiskBackupKind.Differential, false);
        await Execute(connection, $"UPDATE [{scope.SourceName}].dbo.ChainProbe SET Value=3");
        var firstLog = await provider.BackupToDedicatedDiskAsync(connectionString, scope.SourceName, scope.BackupDirectory, SqlServerDiskBackupKind.Log, false);
        await Execute(connection, $"UPDATE [{scope.SourceName}].dbo.ChainProbe SET Value=4");
        var lastLog = await provider.BackupToDedicatedDiskAsync(connectionString, scope.SourceName, scope.BackupDirectory, SqlServerDiskBackupKind.Log, false);
        var files = await provider.ReadDiskBackupFileListAsync(connectionString, full.ServerBackupPath);
        var destinations = files.ToDictionary(file => file.LogicalName,
            file => Path.Combine(scope.BackupDirectory, scope.RestoreName + (file.FileType == "L" ? ".ldf" : ".mdf")));
        scope.RecordRestoreFiles(destinations.Values);
        using var telemetry = DbaClientXDiagnostics.StartOperation("restore-chain", null);
        await Assert.ThrowsAsync<InvalidDataException>(() => provider.PrepareRestoreChainAsNewAsync(connectionString,
            Sources(full, differential, lastLog), scope.RestoreName, destinations));
        await Assert.ThrowsAsync<InvalidDataException>(() => provider.PrepareRestoreChainAsNewAsync(connectionString,
            Sources(copy, differential, firstLog, lastLog), scope.RestoreName, destinations));
        Assert.False(await SqlServerRecoveryTestScope.DatabaseExistsAsync(connection, scope.RestoreName));
        var plan = await provider.PrepareRestoreChainAsNewAsync(connectionString, Sources(full, differential, firstLog, lastLog), scope.RestoreName, destinations);
        destinations.Clear(); // Preparation owns its relocation snapshot.
        Assert.Equal(4, plan.Steps.Count);
        Assert.All(plan.RequiredFiles, file => { Assert.NotNull(file.UniqueId); Assert.NotNull(file.FileId); });
        Assert.True(plan.RequiredFileBytes >= files.Sum(file => file.SizeBytes));
        await provider.RestoreChainAsNewAsync(connectionString, plan);
        using var read = new SqlCommand($"SELECT Value FROM [{scope.RestoreName}].dbo.ChainProbe", connection);
        Assert.Equal(4, Convert.ToInt32(await read.ExecuteScalarAsync()));
        Assert.True((await provider.CheckDatabaseIntegrityAsync(connectionString, scope.RestoreName)).Succeeded);
        Assert.Equal(0, telemetry.Telemetry.RetryCount);
        await Assert.ThrowsAsync<InvalidOperationException>(() => provider.RestoreChainAsNewAsync(connectionString, plan));
    }

    [Fact]
    [Trait("Category", "LiveSqlRecovery")]
    public async Task CancellationAfterFirstNoRecoveryRetainsOwnedTargetAndReleasesLock()
    {
        var (connectionString, directory) = Configuration();
        await using var scope = new SqlServerRecoveryTestScope(connectionString, directory);
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await scope.CreateSourceAsync(connection);
        using var provider = new ReplayTrackingProvider();
        var full = await provider.BackupDatabaseCopyOnlyToDiskAsync(connectionString, scope.SourceName, scope.BackupDirectory);
        var files = await provider.ReadDiskBackupFileListAsync(connectionString, full.ServerBackupPath);
        var destinations = files.ToDictionary(file => file.LogicalName,
            file => Path.Combine(scope.BackupDirectory, scope.RestoreName + (file.FileType == "L" ? ".ldf" : ".mdf")));
        scope.RecordRestoreFiles(destinations.Values);
        var plan = await provider.PrepareRestoreChainAsNewAsync(connectionString, Sources(full), scope.RestoreName, destinations);
        using var cancellation = new CancellationTokenSource();
        int cancelled = 0;
        string? operationId = null;
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == DbaClientXDiagnostics.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = activity =>
            {
                if (Volatile.Read(ref cancelled) != 0 || activity.OperationName != "DbaClientX.Command"
                    || activity.TraceId.ToString() != operationId
                    || activity.GetTagItem("dbaclientx.outcome") as string != "success") return;
                using var observer = new SqlConnection(connectionString);
                observer.Open();
                using var state = new SqlCommand("SELECT state_desc FROM sys.databases WHERE name=@name", observer) { CommandTimeout = 5 };
                state.Parameters.Add("@name", SqlDbType.NVarChar, 128).Value = scope.RestoreName;
                if (string.Equals(state.ExecuteScalar() as string, "RESTORING", StringComparison.Ordinal))
                { Interlocked.Exchange(ref cancelled, 1); cancellation.Cancel(); }
            }
        };
        ActivitySource.AddActivityListener(listener);
        using var telemetry = DbaClientXDiagnostics.StartOperation("restore-chain-cancel", null);
        operationId = telemetry.OperationId;
        var failure = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => provider.RestoreChainAsNewAsync(connectionString, plan,
            cancellationToken: cancellation.Token).WaitAsync(TimeSpan.FromSeconds(30)));
        Assert.Equal(cancellation.Token, failure.CancellationToken);
        Assert.Equal(1, cancelled);
        Assert.Equal(0, telemetry.Telemetry.RetryCount);
        using var query = new SqlCommand("SELECT state_desc FROM sys.databases WHERE name=@name", connection);
        query.Parameters.Add("@name", SqlDbType.NVarChar, 128).Value = scope.RestoreName;
        Assert.Equal("RESTORING", await query.ExecuteScalarAsync());
        using var checkLock = new SqlCommand("DECLARE @resource nvarchar(255)=N'DbaClientX.restore.'+CONVERT(nvarchar(20),CHECKSUM(@name)); DECLARE @result int; EXEC @result=sys.sp_getapplock @Resource=@resource,@LockMode='Exclusive',@LockOwner='Session',@LockTimeout=1000; IF @result>=0 EXEC sys.sp_releaseapplock @Resource=@resource,@LockOwner='Session'; SELECT @result", connection);
        checkLock.Parameters.Add("@name", SqlDbType.NVarChar, 128).Value = scope.RestoreName;
        Assert.True(Convert.ToInt32(await checkLock.ExecuteScalarAsync()) >= 0);
    }

    [Fact]
    [Trait("Category", "LiveSqlRecovery")]
    public async Task RecreatedFileWithSameLogicalNameIsRefusedBeforeTargetCreation()
    {
        var (connectionString, directory) = Configuration();
        await using var scope = new SqlServerRecoveryTestScope(connectionString, directory);
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await scope.CreateSourceAsync(connection);
        string initial = Path.Combine(scope.BackupDirectory, "initial.ndf");
        string replacement = Path.Combine(scope.BackupDirectory, "replacement.ndf");
        await Execute(connection, $"ALTER DATABASE [{scope.SourceName}] SET RECOVERY FULL");
        scope.RecordAdditionalSourceFile(initial);
        await Execute(connection, $"ALTER DATABASE [{scope.SourceName}] ADD FILE(NAME=N'Secondary',FILENAME=N'{initial.Replace("'", "''")}',SIZE=8MB)");
        using var provider = new SqlServer();
        var full = await provider.BackupToDedicatedDiskAsync(connectionString, scope.SourceName, scope.BackupDirectory, SqlServerDiskBackupKind.Full, false);
        var baseFiles = await provider.ReadDiskBackupFileListAsync(connectionString, full.ServerBackupPath);
        await Execute(connection, $"USE [{scope.SourceName}]; DBCC SHRINKFILE(N'Secondary',EMPTYFILE); USE master; ALTER DATABASE [{scope.SourceName}] REMOVE FILE [Secondary]");
        // SQL Server retains a dropped file's identity until the following log backup permits reuse.
        await provider.BackupToDedicatedDiskAsync(connectionString, scope.SourceName, scope.BackupDirectory, SqlServerDiskBackupKind.Log, false);
        scope.RecordAdditionalSourceFile(replacement);
        await Execute(connection, $"ALTER DATABASE [{scope.SourceName}] ADD FILE(NAME=N'Secondary',FILENAME=N'{replacement.Replace("'", "''")}',SIZE=8MB)");
        var differential = await provider.BackupToDedicatedDiskAsync(connectionString, scope.SourceName, scope.BackupDirectory, SqlServerDiskBackupKind.Differential, false);
        var changed = await provider.ReadDiskBackupFileListAsync(connectionString, differential.ServerBackupPath);
        Assert.Equal(baseFiles.Count, changed.Count);
        Assert.NotEqual(baseFiles.Single(file => file.LogicalName == "Secondary").UniqueId,
            changed.Single(file => file.LogicalName == "Secondary").UniqueId);
        var destinations = baseFiles.Select((file, i) => (file.LogicalName, Path: Path.Combine(scope.BackupDirectory, "target" + i + ".mdf")))
            .ToDictionary(item => item.LogicalName, item => item.Path);
        scope.RecordRestoreFiles(destinations.Values);
        await Assert.ThrowsAsync<InvalidDataException>(() => provider.PrepareRestoreChainAsNewAsync(connectionString,
            Sources(full, differential), scope.RestoreName, destinations));
        Assert.False(await SqlServerRecoveryTestScope.DatabaseExistsAsync(connection, scope.RestoreName));
        Assert.True(await SqlServerRecoveryTestScope.DatabaseExistsAsync(connection, scope.SourceName));
    }

    private static (string Connection, string Directory) Configuration()
    {
        var connection = Environment.GetEnvironmentVariable("DBACLIENTX_SQL_BACKUP_TEST_CONNECTION");
        var directory = Environment.GetEnvironmentVariable("DBACLIENTX_SQL_BACKUP_TEST_DIRECTORY");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connection) || string.IsNullOrWhiteSpace(directory), "Set local SQL backup connection and directory for native recovery proof.");
        return (new SqlConnectionStringBuilder(connection) { InitialCatalog = "master", Enlist = false, Pooling = false }.ConnectionString, directory!);
    }

    private static SqlServerRestoreChainSource[] Sources(params SqlServerDiskBackupResult[] backups)
        => backups.Select(backup => new SqlServerRestoreChainSource(backup.ServerBackupPath, backup.Header.Identity)).ToArray();
    private static async Task Execute(SqlConnection connection, string sql)
    { using var command = new SqlCommand(sql, connection); await command.ExecuteNonQueryAsync(); }
    private sealed class ReplayTrackingProvider : SqlServer
    {
        public ReplayTrackingProvider() { CommandRetryMode = CommandRetryMode.ReplaySafe; MaxRetryAttempts = 5; }
        protected override bool IsTransient(Exception exception) => true;
    }
}
