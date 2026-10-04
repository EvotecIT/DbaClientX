using System.Data;
using System.Diagnostics;
using DBAClientX;
using DBAClientX.Diagnostics;
using Microsoft.Data.SqlClient;

namespace DbaClientX.Tests;

public sealed class SqlServerRecoveryInFlightCancellationTests
{
    private readonly ITestOutputHelper _output;
    public SqlServerRecoveryInFlightCancellationTests(ITestOutputHelper output) => _output = output;

    [Theory]
    [InlineData("backup")]
    [InlineData("single-full")]
    [InlineData("chain")]
    [Trait("Category", "LiveSqlRecovery")]
    public async Task RunningRecoveryCommandPreservesCallerCancellationWithoutReplay(string operation)
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQL_BACKUP_TEST_CONNECTION");
        string? directory = Environment.GetEnvironmentVariable("DBACLIENTX_SQL_BACKUP_TEST_DIRECTORY");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString) || string.IsNullOrWhiteSpace(directory),
            "Set local SQL recovery connection and backup directory. DMV observation requires server-state permission.");
        var builder = new SqlConnectionStringBuilder(connectionString)
        { InitialCatalog = "master", Pooling = false, Enlist = false,
            ApplicationName = "DbaClientXRecoveryCancellation_" + Guid.NewGuid().ToString("N") };
        await using var scope = new SqlServerRecoveryTestScope(builder.ConnectionString, directory!);
        await using var observer = new SqlConnection(builder.ConnectionString);
        await observer.OpenAsync();
        await scope.CreateSourceAsync(observer);
        using (var fill = new SqlCommand($"CREATE TABLE [{scope.SourceName}].dbo.CancelProbe(Payload varbinary(8000) NOT NULL); "
            + $"INSERT INTO [{scope.SourceName}].dbo.CancelProbe WITH(TABLOCK) SELECT TOP(4096) CRYPT_GEN_RANDOM(8000) FROM sys.all_objects a CROSS JOIN sys.all_objects b", observer)
            { CommandTimeout = 60 })
            await fill.ExecuteNonQueryAsync();
        using var provider = new ReplayTrackingProvider(_output);
        using var caller = new CancellationTokenSource();
        SqlServerRestorePlan? singlePlan = null;
        SqlServerRestoreChainPlan? chainPlan = null;
        if (operation != "backup")
        {
            var backup = await provider.BackupDatabaseCopyOnlyToDiskAsync(builder.ConnectionString, scope.SourceName, scope.BackupDirectory);
            var files = await provider.ReadDiskBackupFileListAsync(builder.ConnectionString, backup.ServerBackupPath);
            var destinations = files.Select((file, index) => (file.LogicalName, Path: Path.Combine(scope.BackupDirectory,
                "target" + index + (file.FileType == "L" ? ".ldf" : ".mdf"))))
                .ToDictionary(file => file.LogicalName, file => file.Path);
            scope.RecordRestoreFiles(destinations.Values);
            if (operation == "single-full")
                singlePlan = await provider.PrepareRestoreAsNewAsync(builder.ConnectionString, backup.ServerBackupPath,
                    scope.RestoreName, backup.Header.Identity, destinations);
            else
                chainPlan = await provider.PrepareRestoreChainAsNewAsync(builder.ConnectionString,
                    new[] { new SqlServerRestoreChainSource(backup.ServerBackupPath, backup.Header.Identity) }, scope.RestoreName, destinations);
        }
        using var telemetry = DbaClientXDiagnostics.StartOperation("recovery-in-flight-cancellation", null);
        Task command = operation switch
        {
            "backup" => provider.BackupDatabaseCopyOnlyToDiskAsync(builder.ConnectionString, scope.SourceName,
                scope.BackupDirectory, cancellationToken: caller.Token),
            "single-full" => provider.RestoreDatabaseAsNewAsync(builder.ConnectionString, singlePlan!, cancellationToken: caller.Token),
            _ => provider.RestoreChainAsNewAsync(builder.ConnectionString, chainPlan!, cancellationToken: caller.Token)
        };
        string nativeCommand = operation == "backup" ? "BACKUP DATABASE" : "RESTORE DATABASE";
        int? observedSession = null;
        var deadline = Stopwatch.StartNew();
        try
        {
            using var request = new SqlCommand("SELECT TOP(1) r.session_id FROM sys.dm_exec_requests r "
                + "JOIN sys.dm_exec_sessions s ON r.session_id=s.session_id "
                + "WHERE r.command=@command AND s.program_name=@application", observer) { CommandTimeout = 5 };
            request.Parameters.Add("@command", SqlDbType.NVarChar, 60).Value = nativeCommand;
            request.Parameters.Add("@application", SqlDbType.NVarChar, 128).Value = builder.ApplicationName;
            while (!command.IsCompleted && deadline.Elapsed < TimeSpan.FromSeconds(30))
            {
                object? result = await request.ExecuteScalarAsync();
                if (result is not null && result is not DBNull)
                {
                    observedSession = Convert.ToInt32(result);
                    _output.WriteLine("Observed running native " + nativeCommand + " session " + observedSession);
                    caller.Cancel();
                    break;
                }
                await Task.Delay(1);
            }
            Assert.True(observedSession.HasValue, "The operation completed without observing its running native command; in-flight cancellation was not qualified.");
            var failure = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => command.WaitAsync(TimeSpan.FromSeconds(30)));
            Assert.Equal(caller.Token, failure.CancellationToken);
            Assert.Equal(0, telemetry.Telemetry.RetryCount);
            using var remaining = new SqlCommand("SELECT COUNT(*) FROM sys.dm_exec_requests WHERE session_id=@session AND command=@command", observer);
            remaining.Parameters.Add("@session", SqlDbType.Int).Value = observedSession.Value;
            remaining.Parameters.Add("@command", SqlDbType.NVarChar, 60).Value = nativeCommand;
            Assert.Equal(0, Convert.ToInt32(await remaining.ExecuteScalarAsync()));
            if (operation != "backup")
            {
                using var checkLock = new SqlCommand("DECLARE @resource nvarchar(255)=N'DbaClientX.restore.'+CONVERT(nvarchar(20),CHECKSUM(@name)); "
                    + "DECLARE @result int; EXEC @result=sys.sp_getapplock @Resource=@resource,@LockMode='Exclusive',@LockOwner='Session',@LockTimeout=1000; "
                    + "IF @result>=0 EXEC sys.sp_releaseapplock @Resource=@resource,@LockOwner='Session'; SELECT @result", observer);
                checkLock.Parameters.Add("@name", SqlDbType.NVarChar, 128).Value = scope.RestoreName;
                Assert.True(Convert.ToInt32(await checkLock.ExecuteScalarAsync()) >= 0);
            }
        }
        finally
        {
            caller.Cancel();
            // Observe the operation's completion before allowing the owned fixture to delete server files.
            try { await command.WaitAsync(TimeSpan.FromSeconds(30)); }
            catch (Exception) when (command.IsCompleted) { }
        }
    }

    private sealed class ReplayTrackingProvider : SqlServer
    {
        private readonly ITestOutputHelper _output;
        public ReplayTrackingProvider(ITestOutputHelper output) { _output = output; CommandRetryMode = CommandRetryMode.ReplaySafe; MaxRetryAttempts = 5; }
        protected override bool IsTransient(Exception exception) => true;
        protected override bool IsProviderCancellationException(Exception exception)
        {
            if (exception is SqlException sql)
                _output.WriteLine("Native error metadata: " + string.Join(",", sql.Errors.Cast<SqlError>().Select(error => $"{error.Number}/{error.Class}/{error.State}")));
            return base.IsProviderCancellationException(exception);
        }
    }
}
