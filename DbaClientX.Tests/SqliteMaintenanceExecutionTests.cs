using DBAClientX;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

public sealed class SqliteMaintenanceExecutionTests
{
    [Fact]
    public async Task CheckIntegrityAsync_HealthyDatabase_ReturnsHealthyResult()
    {
        string database = CreateDatabase();
        try
        {
            using var sqlite = new SQLite();

            SqliteIntegrityCheckResult result = await sqlite.CheckIntegrityAsync(database, fullCheck: true);

            Assert.True(result.IsHealthy);
            Assert.True(result.IsFullCheck);
            Assert.Empty(result.Issues);
            Assert.True(result.Elapsed >= TimeSpan.Zero);
        }
        finally
        {
            Cleanup(database);
        }
    }

    [Fact]
    public async Task BackupDatabaseIncrementalAsync_CompletedBackup_IsReadableAndReportsProgress()
    {
        string source = CreateDatabase(rowCount: 256);
        string destination = Path.Combine(Path.GetTempPath(), $"dbaclientx-backup-{Guid.NewGuid():N}.sqlite");
        var reports = new List<SqliteBackupProgress>();
        try
        {
            using var sqlite = new SQLite();
            var progress = new InlineProgress<SqliteBackupProgress>(reports.Add);

            SqliteBackupResult result = await sqlite.BackupDatabaseIncrementalAsync(
                source,
                destination,
                new SqliteBackupOptions { PagesPerStep = 4 },
                progress);

            Assert.True(File.Exists(destination));
            Assert.Equal(new FileInfo(destination).Length, result.DestinationLengthBytes);
            Assert.True(result.CopiedPages > 0);
            Assert.NotEmpty(reports);
            Assert.Equal(100d, reports[^1].PercentComplete);

            await using var connection = new SqliteConnection(SQLite.BuildConnectionString(destination));
            await connection.OpenAsync();
            await using SqliteCommand command = connection.CreateCommand();
            command.CommandText = "SELECT COUNT(*) FROM backup_contract;";
            long count = (long)(await command.ExecuteScalarAsync())!;
            Assert.Equal(256, count);
        }
        finally
        {
            Cleanup(source);
            Cleanup(destination);
        }
    }

    [Fact]
    public async Task BackupDatabaseIncrementalAsync_CanceledBackup_DeletesIncompleteDestination()
    {
        string source = CreateDatabase(rowCount: 512);
        string destination = Path.Combine(Path.GetTempPath(), $"dbaclientx-canceled-{Guid.NewGuid():N}.sqlite");
        using var cancellationSource = new CancellationTokenSource();
        try
        {
            using var sqlite = new SQLite();
            var progress = new InlineProgress<SqliteBackupProgress>(value =>
            {
                if (value.CopiedPages > 0)
                {
                    cancellationSource.Cancel();
                }
            });

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sqlite.BackupDatabaseIncrementalAsync(
                source,
                destination,
                new SqliteBackupOptions
                {
                    PagesPerStep = 1,
                    StepDelay = TimeSpan.FromMilliseconds(5)
                },
                progress,
                cancellationSource.Token));

            Assert.False(File.Exists(destination));
        }
        finally
        {
            Cleanup(source);
            Cleanup(destination);
        }
    }

    [Fact]
    public async Task BackupDatabaseIncrementalAsync_CanceledOverwrite_PreservesExistingDestination()
    {
        string source = CreateDatabase(rowCount: 512);
        string destination = CreateDatabase(rowCount: 1);
        using var cancellationSource = new CancellationTokenSource();
        try
        {
            using var sqlite = new SQLite();
            var progress = new InlineProgress<SqliteBackupProgress>(value =>
            {
                if (value.CopiedPages > 0)
                {
                    cancellationSource.Cancel();
                }
            });

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sqlite.BackupDatabaseIncrementalAsync(
                source,
                destination,
                new SqliteBackupOptions
                {
                    PagesPerStep = 1,
                    StepDelay = TimeSpan.FromMilliseconds(5),
                    OverwriteDestination = true
                },
                progress,
                cancellationSource.Token));

            Assert.Equal(1, await CountRowsAsync(destination));
            string directory = Path.GetDirectoryName(destination)!;
            Assert.Empty(Directory.GetFiles(directory, $"{Path.GetFileName(destination)}.*.partial"));
        }
        finally
        {
            Cleanup(source);
            Cleanup(destination);
        }
    }

    [Fact]
    public async Task BackupDatabaseIncrementalAsync_CompletedOverwrite_AtomicallyReplacesDestination()
    {
        string source = CreateDatabase(rowCount: 128);
        string destination = CreateDatabase(rowCount: 1);
        try
        {
            using var sqlite = new SQLite();

            await sqlite.BackupDatabaseIncrementalAsync(
                source,
                destination,
                new SqliteBackupOptions
                {
                    PagesPerStep = 4,
                    OverwriteDestination = true
                });

            Assert.Equal(128, await CountRowsAsync(destination));
            string directory = Path.GetDirectoryName(destination)!;
            Assert.Empty(Directory.GetFiles(directory, $"{Path.GetFileName(destination)}.*.partial"));
        }
        finally
        {
            Cleanup(source);
            Cleanup(destination);
        }
    }

    [Fact]
    public async Task BackupDatabaseIncrementalAsync_CompletedOverwrite_RemovesStaleDestinationSidecars()
    {
        string source = CreateDatabase(rowCount: 128);
        string destination = CreateDatabase(rowCount: 1);
        try
        {
            File.WriteAllText(destination + "-wal", "stale-wal");
            File.WriteAllText(destination + "-shm", "stale-shm");
            using var sqlite = new SQLite();

            await sqlite.BackupDatabaseIncrementalAsync(
                source,
                destination,
                new SqliteBackupOptions { OverwriteDestination = true });

            Assert.False(File.Exists(destination + "-wal"));
            Assert.False(File.Exists(destination + "-shm"));
            Assert.Equal(128, await CountRowsAsync(destination));
        }
        finally
        {
            Cleanup(source);
            Cleanup(destination);
        }
    }

    [Fact]
    public async Task BackupDatabaseIncrementalAsync_ActiveClientTransaction_FailsBeforeCreatingDestination()
    {
        string source = CreateDatabase();
        string destination = Path.Combine(Path.GetTempPath(), $"dbaclientx-transaction-backup-{Guid.NewGuid():N}.sqlite");
        try
        {
            using var sqlite = new SQLite();
            sqlite.BeginTransaction(source);

            await Assert.ThrowsAsync<DbaTransactionException>(() => sqlite.BackupDatabaseIncrementalAsync(source, destination));
            Assert.False(File.Exists(destination));

            sqlite.Rollback();
        }
        finally
        {
            Cleanup(source);
            Cleanup(destination);
        }
    }

    [Fact]
    public async Task CheckIntegrityAsync_ActiveClientTransaction_FailsBeforeStartingMaintenance()
    {
        string source = CreateDatabase();
        try
        {
            using var sqlite = new SQLite();
            sqlite.BeginTransaction(source);

            await Assert.ThrowsAsync<DbaTransactionException>(() => sqlite.CheckIntegrityAsync(source));

            sqlite.Rollback();
        }
        finally
        {
            Cleanup(source);
        }
    }

    [Fact]
    public async Task BackupDatabaseIncrementalAsync_UnboundedNativeStepOptions_AreRejected()
    {
        string source = CreateDatabase();
        string destination = Path.Combine(Path.GetTempPath(), $"dbaclientx-option-bounds-{Guid.NewGuid():N}.sqlite");
        try
        {
            using var sqlite = new SQLite();

            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => sqlite.BackupDatabaseIncrementalAsync(
                source,
                destination,
                new SqliteBackupOptions { PagesPerStep = 4097 }));
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => sqlite.BackupDatabaseIncrementalAsync(
                source,
                destination,
                new SqliteBackupOptions { BusyTimeoutMs = 1001 }));
        }
        finally
        {
            Cleanup(source);
            Cleanup(destination);
        }
    }

    [Theory]
    [InlineData(null, 5000, 1000)]
    [InlineData(null, 250, 250)]
    [InlineData(750, 5000, 750)]
    public void ResolveBackupBusyTimeoutMs_DefaultAndExplicitValues_AreBounded(
        int? requested,
        int instance,
        int expected)
    {
        var method = typeof(SQLite).GetMethod(
            "ResolveBackupBusyTimeoutMs",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;

        Assert.Equal(expected, method.Invoke(null, new object?[] { requested, instance }));
    }

    [Fact]
    public async Task MaintenanceAsync_PreCanceledToken_DoesNotCreateWorkOrDestination()
    {
        string source = CreateDatabase();
        string destination = Path.Combine(Path.GetTempPath(), $"dbaclientx-precanceled-{Guid.NewGuid():N}.sqlite");
        using var cancellationSource = new CancellationTokenSource();
        cancellationSource.Cancel();
        try
        {
            using var sqlite = new SQLite();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sqlite.CheckIntegrityAsync(
                source,
                cancellationToken: cancellationSource.Token));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sqlite.BackupDatabaseIncrementalAsync(
                source,
                destination,
                cancellationToken: cancellationSource.Token));

            Assert.False(File.Exists(destination));
        }
        finally
        {
            Cleanup(source);
            Cleanup(destination);
        }
    }

    private static string CreateDatabase(int rowCount = 1)
    {
        string path = Path.Combine(Path.GetTempPath(), $"dbaclientx-maintenance-{Guid.NewGuid():N}.sqlite");
        using var connection = new SqliteConnection(SQLite.BuildConnectionString(path));
        connection.Open();
        using SqliteCommand command = connection.CreateCommand();
        command.CommandText = "CREATE TABLE backup_contract(id INTEGER PRIMARY KEY, payload BLOB NOT NULL);";
        command.ExecuteNonQuery();
        using SqliteTransaction transaction = connection.BeginTransaction();
        command.Transaction = transaction;
        command.CommandText = "INSERT INTO backup_contract(payload) VALUES(randomblob(4096));";
        for (int index = 0; index < rowCount; index++)
        {
            command.ExecuteNonQuery();
        }
        transaction.Commit();
        return path;
    }

    private static async Task<long> CountRowsAsync(string database)
    {
        await using var connection = new SqliteConnection(SQLite.BuildConnectionString(database));
        await connection.OpenAsync();
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM backup_contract;";
        return (long)(await command.ExecuteScalarAsync())!;
    }

    private static void Cleanup(string path)
    {
        foreach (string suffix in new[] { string.Empty, "-wal", "-shm" })
        {
            string candidate = path + suffix;
            if (File.Exists(candidate))
            {
                File.Delete(candidate);
            }
        }
    }

    private sealed class InlineProgress<T> : IProgress<T>
    {
        private readonly Action<T> _handler;

        public InlineProgress(Action<T> handler)
        {
            _handler = handler;
        }

        public void Report(T value)
        {
            _handler(value);
        }
    }
}
