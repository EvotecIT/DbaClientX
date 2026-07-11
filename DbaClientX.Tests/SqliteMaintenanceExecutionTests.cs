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
