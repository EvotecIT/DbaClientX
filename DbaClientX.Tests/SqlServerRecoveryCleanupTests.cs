using DBAClientX;
using Microsoft.Data.SqlClient;
using System.Data;

namespace DbaClientX.Tests;

public sealed class SqlServerRecoveryCleanupTests
{
    [Theory]
    [InlineData("Backup")]
    [InlineData("Restore")]
    [InlineData("Restoring")]
    [Trait("Category", "LiveSqlRecovery")]
    public async Task Scope_CleansOwnedResourcesAfterUnsuccessfulOutcomes(string stage)
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQL_BACKUP_TEST_CONNECTION");
        string? backupParent = Environment.GetEnvironmentVariable("DBACLIENTX_SQL_BACKUP_TEST_DIRECTORY");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString) || string.IsNullOrWhiteSpace(backupParent),
            "Set the local SQL recovery connection and backup directory.");
        var builder = new SqlConnectionStringBuilder(connectionString)
        { InitialCatalog = "master", Enlist = false, Pooling = false };
        using var connection = new SqlConnection(builder.ConnectionString);
        await connection.OpenAsync();
        var scope = new SqlServerRecoveryTestScope(connectionString!, backupParent!);
        var paths = new List<string>();
        await using (scope)
        {
            await scope.CreateSourceAsync(connection);
            if (stage == "Backup")
            {
                using var provider = new FailAfterBackup();
                await Assert.ThrowsAsync<IOException>(() => provider.BackupDatabaseCopyOnlyToDiskAsync(
                    connectionString!, scope.SourceName, scope.BackupDirectory));
                Assert.Single(Directory.GetFiles(scope.BackupDirectory));
            }
            else
            {
                using var provider = new FailAfterRestore();
                var backup = await provider.BackupDatabaseCopyOnlyToDiskAsync(
                    connectionString!, scope.SourceName, scope.BackupDirectory);
                var files = await provider.ReadDiskBackupFileListAsync(connectionString!, backup.ServerBackupPath);
                string? directory = Environment.GetEnvironmentVariable("DBACLIENTX_SQL_BACKUP_TEST_RESTORE_DIRECTORY");
                if (directory is null)
                {
                    using var location = new SqlCommand("SELECT CONVERT(nvarchar(4000), SERVERPROPERTY('InstanceDefaultDataPath'))", connection);
                    directory = (string)(await location.ExecuteScalarAsync())!;
                }
                var destinations = new Dictionary<string, string>();
                for (int index = 0; index < files.Count; index++)
                {
                    string path = Path.Combine(directory, scope.RestoreName + "_" + index
                        + (files[index].FileType == "L" ? ".ldf" : ".mdf"));
                    destinations.Add(files[index].LogicalName, path);
                    paths.Add(path);
                }
                scope.RecordRestoreFiles(paths);
                if (stage == "Restore")
                {
                    await Assert.ThrowsAsync<IOException>(() => provider.RestoreDatabaseAsNewAsync(
                        connectionString!, backup.ServerBackupPath, scope.RestoreName, backup.Header.Identity, destinations));
                }
                else
                {
                    // Exercise actual SQL Server RESTORING state without introducing a product-only fault hook.
                    string moves = string.Join(", ", destinations.Select(item =>
                        "MOVE N'" + item.Key.Replace("'", "''") + "' TO N'" + item.Value.Replace("'", "''") + "'"));
                    using var restore = new SqlCommand("RESTORE DATABASE [" + scope.RestoreName
                        + "] FROM DISK = @path WITH FILE = 1, NORECOVERY, CHECKSUM, " + moves, connection);
                    restore.Parameters.Add("@path", SqlDbType.NVarChar, 4000).Value = backup.ServerBackupPath;
                    await restore.ExecuteNonQueryAsync();
                    using var state = new SqlCommand("SELECT state_desc FROM sys.databases WHERE name = @name", connection);
                    state.Parameters.Add("@name", SqlDbType.NVarChar, 128).Value = scope.RestoreName;
                    Assert.Equal("RESTORING", await state.ExecuteScalarAsync());
                }
                Assert.True(await SqlServerRecoveryTestScope.DatabaseExistsAsync(connection, scope.RestoreName));
                Assert.All(paths, path => Assert.True(File.Exists(path)));
            }
        }
        Assert.False(await SqlServerRecoveryTestScope.DatabaseExistsAsync(connection, scope.SourceName));
        Assert.False(await SqlServerRecoveryTestScope.DatabaseExistsAsync(connection, scope.RestoreName));
        Assert.All(paths, path => Assert.False(File.Exists(path)));
        Assert.False(Directory.Exists(scope.BackupDirectory));
    }

    [Fact]
    [Trait("Category", "LiveSqlRecovery")]
    public async Task Scope_RetainsBackupDirectoryWhenDatabaseFileOwnershipChanges()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQL_BACKUP_TEST_CONNECTION");
        string? backupParent = Environment.GetEnvironmentVariable("DBACLIENTX_SQL_BACKUP_TEST_DIRECTORY");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString) || string.IsNullOrWhiteSpace(backupParent),
            "Set the local SQL recovery connection and backup directory.");
        var builder = new SqlConnectionStringBuilder(connectionString)
        { InitialCatalog = "master", Enlist = false, Pooling = false };
        using var connection = new SqlConnection(builder.ConnectionString);
        await connection.OpenAsync();
        var scope = new SqlServerRecoveryTestScope(builder.ConnectionString, backupParent!);
        try
        {
            await scope.CreateSourceAsync(connection);
            using var provider = new SqlServer();
            var backup = await provider.BackupDatabaseCopyOnlyToDiskAsync(builder.ConnectionString,
                scope.SourceName, scope.BackupDirectory);
            string extra = Path.Combine(scope.BackupDirectory, "outside-recorded-inventory.ndf");
            Assert.False(File.Exists(extra));
            // Deliberately change the owned database's inventory without recording that file
            // in the scope; cleanup must preserve the directory when its DROP guard refuses.
            using var add = new SqlCommand($"ALTER DATABASE [{scope.SourceName}] ADD FILE "
                + $"(NAME=N'UnexpectedFile',FILENAME=N'{extra.Replace("'", "''")}',SIZE=8MB)", connection);
            await add.ExecuteNonQueryAsync();
            await Assert.ThrowsAsync<AggregateException>(() => scope.DisposeAsync().AsTask());
            Assert.True(await SqlServerRecoveryTestScope.DatabaseExistsAsync(connection, scope.SourceName));
            Assert.True(Directory.Exists(scope.BackupDirectory));
            Assert.True(File.Exists(backup.ServerBackupPath));
            Assert.True(File.Exists(extra));
        }
        finally
        {
            // This test itself owns the extra file and generated source. Remove it natively,
            // then let the ordinary scope finish only after the database no longer exists.
            if (await SqlServerRecoveryTestScope.DatabaseExistsAsync(connection, scope.SourceName))
            {
                using var drop = new SqlCommand($"DROP DATABASE [{scope.SourceName}]", connection);
                await drop.ExecuteNonQueryAsync();
            }
            await scope.DisposeAsync();
        }
        Assert.False(Directory.Exists(scope.BackupDirectory));
    }

    [Fact]
    public async Task Cleanup_AttemptsEveryIndependentActionAndReportsAllFailures()
    {
        var calls = new List<int>();
        var first = new IOException("First cleanup failure.");
        var last = new InvalidOperationException("Last cleanup failure.");
        var error = await Assert.ThrowsAsync<AggregateException>(() => SqlServerRecoveryTestScope.RunCleanupAsync(new Func<Task>[]
        {
            () => { calls.Add(1); throw first; },
            () => { calls.Add(2); return Task.CompletedTask; },
            () => { calls.Add(3); throw last; }
        }));
        Assert.Equal(new[] { 1, 2, 3 }, calls);
        Assert.Equal(new Exception[] { first, last }, error.InnerExceptions);
    }

    private sealed class FailAfterBackup : SqlServer
    {
        public override async Task<SqlServerDiskBackupResult> BackupDatabaseCopyOnlyToDiskAsync(
            string connectionString, string databaseName, string serverBackupDirectory,
            int commandTimeoutSeconds = 3600, CancellationToken cancellationToken = default)
        {
            await base.BackupDatabaseCopyOnlyToDiskAsync(connectionString, databaseName, serverBackupDirectory,
                commandTimeoutSeconds, cancellationToken);
            throw new IOException("Injected failure after server-side backup completion.");
        }
    }

    private sealed class FailAfterRestore : SqlServer
    {
        public override async Task RestoreDatabaseAsNewAsync(string connectionString, string serverBackupPath,
            string targetDatabaseName, SqlServerBackupIdentity expectedBackup,
            IReadOnlyDictionary<string, string> fileDestinations, int commandTimeoutSeconds = 3600,
            CancellationToken cancellationToken = default)
        {
            await base.RestoreDatabaseAsNewAsync(connectionString, serverBackupPath, targetDatabaseName,
                expectedBackup, fileDestinations, commandTimeoutSeconds, cancellationToken);
            throw new IOException("Injected failure after server-side restore completion.");
        }
    }
}
