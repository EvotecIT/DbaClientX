using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.Linq;
using System.Threading;
using DBAClientX;
using Microsoft.Data.SqlClient;
using Xunit;

namespace DbaClientX.Tests;

public class SqlServerBackupTests
{
    [Fact]
    [Trait("Category", "LiveSqlRecovery")]
    public async Task BackupDatabaseCopyOnlyToDiskAsync_LocalSqlServer_CreatesReadableBackup()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQL_BACKUP_TEST_CONNECTION");
        string? backupDirectory = Environment.GetEnvironmentVariable("DBACLIENTX_SQL_BACKUP_TEST_DIRECTORY");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString) || string.IsNullOrWhiteSpace(backupDirectory),
            "Set both DBACLIENTX_SQL_BACKUP_TEST_CONNECTION and DBACLIENTX_SQL_BACKUP_TEST_DIRECTORY for the local SQL backup test.");
        Assert.True(Directory.Exists(backupDirectory), "The test process must be able to clean up the SQL backup directory.");

        string databaseName = "DbaClientXBackupTest_" + Guid.NewGuid().ToString("N");
        string restoreName = "DbaClientXRestoreTest_" + Guid.NewGuid().ToString("N");
        string? backupPath = null;
        string? otherBackupPath = null;
        var restoredFiles = new List<string>();
        bool sourceCreated = false;
        bool restoreCreated = false;
        var builder = new SqlConnectionStringBuilder(connectionString) { InitialCatalog = "master" };
        await using var connection = new SqlConnection(builder.ConnectionString);
        await connection.OpenAsync();
        try
        {
            await using (var create = new SqlCommand("CREATE DATABASE [" + databaseName + "]", connection))
            {
                await create.ExecuteNonQueryAsync();
                sourceCreated = true;
            }

            await using (var seed = new SqlCommand(
                "CREATE TABLE [" + databaseName + "].dbo.BackupProbe (Value int NOT NULL); "
                    + "INSERT INTO [" + databaseName + "].dbo.BackupProbe VALUES (42);", connection))
            {
                await seed.ExecuteNonQueryAsync();
            }

            using var provider = new SqlServer();
            SqlServerDiskBackupResult backup = await provider.BackupDatabaseCopyOnlyToDiskAsync(
                connectionString!, databaseName, backupDirectory!);
            backupPath = backup.ServerBackupPath;
            Assert.Equal(databaseName, backup.Header.DatabaseName);
            Assert.NotEqual(Guid.Empty, backup.Header.BackupSetGuid);
            Assert.False(string.IsNullOrWhiteSpace(backup.Header.MediaName));
            Assert.NotEqual(Guid.Empty, backup.Header.MediaSetId);
            Assert.Equal(1, backup.Header.BackupType);
            Assert.True(backup.Header.IsCopyOnly);
            Assert.True(backup.Header.HasBackupChecksums);
            Assert.False(backup.Header.IsDamaged);
            Assert.True(File.Exists(backupPath));
            Assert.True(new FileInfo(backupPath).Length > 0);
            await provider.VerifyDiskBackupAsync(connectionString!, backupPath);
            SqlServerDiskBackupHeader readHeader = await provider.ReadDiskBackupHeaderAsync(
                connectionString!, backupPath);
            Assert.Equal(backup.Header.BackupSetGuid, readHeader.BackupSetGuid);
            Assert.Equal(backup.Header.MediaName, readHeader.MediaName);
            Assert.Equal(backup.Header.MediaSetId, readHeader.MediaSetId);
            SqlServerBackupIdentity expectedBackup = backup.Header.Identity;

            await using (var wrongMedia = new SqlCommand(
                "RESTORE VERIFYONLY FROM DISK = @backupPath WITH FILE = 1, MEDIANAME = @mediaName, CHECKSUM",
                connection))
            {
                wrongMedia.Parameters.Add("@backupPath", System.Data.SqlDbType.NVarChar, 4000).Value = backupPath;
                wrongMedia.Parameters.Add("@mediaName", System.Data.SqlDbType.NVarChar, 128).Value = "WrongMediaName";
                await Assert.ThrowsAsync<SqlException>(() => wrongMedia.ExecuteNonQueryAsync());
            }

            await using (var header = new SqlCommand("RESTORE HEADERONLY FROM DISK = @backupPath", connection))
            {
                header.Parameters.Add("@backupPath", System.Data.SqlDbType.NVarChar, 4000).Value = backupPath;
                await using var reader = await header.ExecuteReaderAsync();
                Assert.True(await reader.ReadAsync());
                Assert.Equal(databaseName, reader["DatabaseName"]);
                Assert.Equal(true, reader["IsCopyOnly"]);
                Assert.Equal(true, reader["HasBackupChecksums"]);
                Assert.False(await reader.ReadAsync());
            }

            string dataDirectory;
            string logDirectory;
            await using (var locations = new SqlCommand(
                "SELECT CONVERT(nvarchar(4000), SERVERPROPERTY('InstanceDefaultDataPath')), "
                    + "CONVERT(nvarchar(4000), SERVERPROPERTY('InstanceDefaultLogPath'))", connection))
            {
                await using var reader = await locations.ExecuteReaderAsync();
                Assert.True(await reader.ReadAsync());
                string? restoreDirectory = Environment.GetEnvironmentVariable("DBACLIENTX_SQL_BACKUP_TEST_RESTORE_DIRECTORY");
                dataDirectory = restoreDirectory ?? reader.GetString(0);
                logDirectory = restoreDirectory ?? reader.GetString(1);
            }

            IReadOnlyList<SqlServerBackupFileInfo> files = await provider.ReadDiskBackupFileListAsync(
                connectionString!, backupPath);
            var occupiedDestinations = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (SqlServerBackupFileInfo file in files)
            {
                occupiedDestinations.Add(file.LogicalName, file.OriginalPhysicalName);
            }

            await Assert.ThrowsAsync<InvalidOperationException>(() => provider.RestoreDatabaseAsNewAsync(
                connectionString!, backupPath, restoreName, expectedBackup, occupiedDestinations));

            var destinations = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            for (int index = 0; index < files.Count; index++)
            {
                SqlServerBackupFileInfo file = files[index];
                string directory = file.FileType == "L" ? logDirectory : dataDirectory;
                string extension = file.FileType == "L" ? ".ldf" : ".mdf";
                string destination = Path.Combine(directory, restoreName + "_" + index + extension);
                Assert.False(File.Exists(destination));
                destinations.Add(file.LogicalName, destination);
                restoredFiles.Add(destination);
            }

            await Assert.ThrowsAsync<InvalidDataException>(() => provider.RestoreDatabaseAsNewAsync(
                connectionString!, backupPath, restoreName,
                new SqlServerBackupIdentity(databaseName, Guid.NewGuid(), expectedBackup.MediaName,
                    expectedBackup.MediaSetId), destinations));
            await Assert.ThrowsAsync<InvalidDataException>(() => provider.RestoreDatabaseAsNewAsync(
                connectionString!, backupPath, restoreName,
                new SqlServerBackupIdentity("OtherSourceDatabase", expectedBackup.BackupSetGuid,
                    expectedBackup.MediaName, expectedBackup.MediaSetId), destinations));
            await Assert.ThrowsAsync<InvalidDataException>(() => provider.RestoreDatabaseAsNewAsync(
                connectionString!, backupPath, restoreName,
                new SqlServerBackupIdentity(databaseName, expectedBackup.BackupSetGuid,
                    "WrongMediaName", expectedBackup.MediaSetId), destinations));
            SqlServerDiskBackupResult otherBackup = await provider.BackupDatabaseCopyOnlyToDiskAsync(
                connectionString!, databaseName, backupDirectory!);
            otherBackupPath = otherBackup.ServerBackupPath;
            Assert.NotEqual(backup.Header.BackupSetGuid, otherBackup.Header.BackupSetGuid);
            await Assert.ThrowsAsync<InvalidDataException>(() => provider.RestoreDatabaseAsNewAsync(
                connectionString!, otherBackupPath, restoreName, expectedBackup, destinations));
            Assert.False(await DatabaseExistsAsync(connection, restoreName));
            Assert.All(restoredFiles, path => Assert.False(File.Exists(path)));

            string collisionPath = restoredFiles[0];
            const string sentinel = "This file must not be overwritten by RESTORE.";
            File.WriteAllText(collisionPath, sentinel);
            try
            {
                var error = await Assert.ThrowsAsync<DbaQueryExecutionException>(() => provider.RestoreDatabaseAsNewAsync(
                    connectionString!, backupPath, restoreName, expectedBackup, destinations));
                Assert.DoesNotContain(collisionPath, error.ToString(), StringComparison.Ordinal);
                Assert.Equal(sentinel, File.ReadAllText(collisionPath));
            }
            finally
            {
                if (File.Exists(collisionPath)) File.Delete(collisionPath);
            }

            var plan = await provider.PrepareRestoreAsNewAsync(connectionString!, backupPath, restoreName,
                expectedBackup, destinations);
            Assert.Equal(expectedBackup.BackupSetGuid, plan.Header.BackupSetGuid);
            Assert.Equal(files.Sum(file => file.SizeBytes), plan.RequiredFileBytes);
            Assert.True(plan.RequiredFileBytes > 0);
            Assert.False(await DatabaseExistsAsync(connection, restoreName));
            Assert.All(restoredFiles, path => Assert.False(File.Exists(path)));
            Assert.Throws<NotSupportedException>(() => ((IDictionary<string, string>)plan.FileDestinations).Clear());
            destinations[files[0].LogicalName] = files[0].OriginalPhysicalName;
            Assert.Equal(restoredFiles[0], plan.FileDestinations[files[0].LogicalName]);

            using (var canceled = new CancellationTokenSource())
            {
                canceled.Cancel();
                var cancellation = await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                    provider.RestoreDatabaseAsNewAsync(connectionString!, plan, cancellationToken: canceled.Token));
                Assert.Equal(canceled.Token, cancellation.CancellationToken);
                Assert.False(await DatabaseExistsAsync(connection, restoreName));
            }

            const string reserveTarget = "DECLARE @resource nvarchar(255) = N'DbaClientX.restore.' + "
                + "CONVERT(nvarchar(20), CHECKSUM(@name)); DECLARE @result int; EXEC @result = sys.sp_getapplock "
                + "@Resource = @resource, @LockMode = 'Exclusive', @LockOwner = 'Session'; SELECT @result;";
            await using (var reserve = new SqlCommand(reserveTarget, connection))
            {
                reserve.Parameters.Add("@name", System.Data.SqlDbType.NVarChar, 128).Value = restoreName;
                Assert.True(Convert.ToInt32(await reserve.ExecuteScalarAsync()) >= 0);
            }
            try
            {
                using var cancellation = new CancellationTokenSource();
                Task waitingRestore = provider.RestoreDatabaseAsNewAsync(connectionString!, plan,
                    cancellationToken: cancellation.Token);
                Assert.NotSame(waitingRestore, await Task.WhenAny(waitingRestore, Task.Delay(200)));
                cancellation.Cancel();
                var canceled = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waitingRestore);
                Assert.Equal(cancellation.Token, canceled.CancellationToken);
                Assert.False(await DatabaseExistsAsync(connection, restoreName));
                Assert.All(restoredFiles, path => Assert.False(File.Exists(path)));
            }
            finally
            {
                await using var release = new SqlCommand(
                    "DECLARE @resource nvarchar(255) = N'DbaClientX.restore.' + CONVERT(nvarchar(20), CHECKSUM(@name)); "
                    + "EXEC sys.sp_releaseapplock @Resource = @resource, @LockOwner = 'Session';", connection);
                release.Parameters.Add("@name", System.Data.SqlDbType.NVarChar, 128).Value = restoreName;
                await release.ExecuteNonQueryAsync();
            }

            async Task<Exception?> TryRestoreAsync()
            {
                try
                {
                    await provider.RestoreDatabaseAsNewAsync(connectionString!, plan);
                    restoreCreated = true;
                    return null;
                }
                catch (Exception exception) { return exception; }
            }
            var attempts = await Task.WhenAll(TryRestoreAsync(), TryRestoreAsync());
            Assert.Single(attempts, error => error is null);
            Assert.Single(attempts, error => error is InvalidOperationException);
            await Assert.ThrowsAsync<InvalidOperationException>(() => provider.RestoreDatabaseAsNewAsync(
                connectionString!, backupPath, restoreName, expectedBackup, destinations));
            await using (var probe = new SqlCommand(
                "SELECT Value FROM [" + restoreName + "].dbo.BackupProbe", connection))
            {
                Assert.Equal(42, await probe.ExecuteScalarAsync());
            }

            var integrity = await provider.CheckDatabaseIntegrityAsync(connectionString!, restoreName);
            Assert.True(integrity.Succeeded);
            Assert.False(integrity.PhysicalOnly);
            Assert.Equal(restoreName, integrity.DatabaseName);
            Assert.Equal(0, integrity.IssueCount);
            Assert.Empty(integrity.Issues);
            var physical = await provider.CheckDatabaseIntegrityAsync(connectionString!, restoreName,
                new SqlServerIntegrityCheckOptions { PhysicalOnly = true, MaxIssues = 1 });
            Assert.True(physical.Succeeded);
            Assert.True(physical.PhysicalOnly);

            await using (var append = new SqlCommand(
                "BACKUP DATABASE @databaseName TO DISK = @backupPath "
                    + "WITH COPY_ONLY, NOINIT, CHECKSUM, STOP_ON_ERROR", connection))
            {
                append.Parameters.Add("@databaseName", System.Data.SqlDbType.NVarChar, 128).Value = databaseName;
                append.Parameters.Add("@backupPath", System.Data.SqlDbType.NVarChar, 4000).Value = backupPath;
                await append.ExecuteNonQueryAsync();
            }

            await Assert.ThrowsAsync<InvalidDataException>(() => provider.ReadDiskBackupHeaderAsync(
                connectionString!, backupPath));
        }
        finally
        {
            try
            {
                await using var dropRestore = new SqlCommand("DROP DATABASE [" + restoreName + "]", connection);
                if (restoreCreated) await dropRestore.ExecuteNonQueryAsync();
            }
            finally
            {
                await using var drop = new SqlCommand("DROP DATABASE [" + databaseName + "]", connection);
                if (sourceCreated) await drop.ExecuteNonQueryAsync();
            }

            foreach (string restoredFile in restoredFiles)
            {
                if (File.Exists(restoredFile)) File.Delete(restoredFile);
            }

            if (backupPath is not null && File.Exists(backupPath))
            {
                File.Delete(backupPath);
            }

            if (otherBackupPath is not null && File.Exists(otherBackupPath))
            {
                File.Delete(otherBackupPath);
            }
        }
    }

    private static async Task<bool> DatabaseExistsAsync(SqlConnection connection, string databaseName)
    {
        await using var command = new SqlCommand("SELECT DB_ID(@databaseName)", connection);
        command.Parameters.Add("@databaseName", System.Data.SqlDbType.NVarChar, 128).Value = databaseName;
        return await command.ExecuteScalarAsync() is not DBNull;
    }
}
