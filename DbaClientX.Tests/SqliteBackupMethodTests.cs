using DBAClientX;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

/// <summary>
/// Covers how a backup chooses between holding a snapshot and copying step-wise, through the selectable
/// <see cref="SQLite.BackupDatabaseAsync"/> and the synchronous <see cref="SQLite.BackupDatabase(string, string, int?)"/>.
/// </summary>
public sealed class SqliteBackupMethodTests
{
    [Theory]
    [InlineData(SqliteBackupMethod.Auto, false)]
    [InlineData(SqliteBackupMethod.Incremental, true)]
    [InlineData(SqliteBackupMethod.Snapshot, true)]
    public async Task BackupDatabaseAsync_CanceledByFinalProgress_DoesNotPublishAndReleasesSource(SqliteBackupMethod method, bool overwrite)
    {
        string source = CreateDatabase(2, wal: true);
        string destination = overwrite ? CreateDatabase(1, wal: false) : NewDestination();
        using var cancellation = new CancellationTokenSource();
        bool finalStep = false;
        try
        {
            using var sqlite = new SQLite();
            var progress = new InlineProgress<SqliteBackupProgress>(value =>
            {
                if (value.CopiedPages > 0 && value.RemainingPages == 0)
                {
                    finalStep = true;
                    cancellation.Cancel();
                }
            });
            var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sqlite.BackupDatabaseAsync(
                source, destination, method,
                new SqliteBackupOptions { PagesPerStep = 4096, OverwriteDestination = overwrite },
                progress, cancellation.Token));

            Assert.True(finalStep);
            Assert.Equal(cancellation.Token, error.CancellationToken);
            if (overwrite) Assert.Equal(1, await CountRowsAsync(destination));
            else Assert.False(File.Exists(destination));
            Assert.Empty(Directory.GetFiles(Path.GetDirectoryName(destination)!, Path.GetFileName(destination) + ".*.partial"));
            sqlite.ExecuteNonQuery(source, "INSERT INTO backup_contract(payload) VALUES(randomblob(16))");
            Assert.Equal(3, await CountRowsAsync(source));
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            Cleanup(source);
            Cleanup(destination);
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task BackupDatabaseAsync_AutoWhileAnotherConnectionWrites_PicksTheMethodFromTheJournalMode(bool wal)
    {
        string source = CreateDatabase(rowCount: 256, wal);
        string destination = NewDestination();
        await using var writer = new SqliteConnection(SQLite.BuildConnectionString(source));
        try
        {
            await writer.OpenAsync();
            int writes = 0;
            var progress = new InlineProgress<SqliteBackupProgress>(value =>
            {
                if (writes < 5 && value.RemainingPages > 0)
                {
                    using SqliteCommand insert = writer.CreateCommand();
                    insert.CommandText = "INSERT INTO backup_contract(payload) VALUES(randomblob(4096));";
                    insert.ExecuteNonQuery();
                    writes++;
                }
            });
            using var sqlite = new SQLite();

            SqliteBackupResult result = await sqlite.BackupDatabaseAsync(
                source,
                destination,
                SqliteBackupMethod.Auto,
                new SqliteBackupOptions { PagesPerStep = 16 },
                progress);

            // WAL: the held snapshot copies the start state. Rollback journal: the writer commits between steps, which
            // the step-wise copy allows and restarts after, so it ends with every row.
            Assert.Equal(5, writes);
            Assert.Equal(wal ? SqliteBackupMethod.Snapshot : SqliteBackupMethod.Incremental, result.Method);
            Assert.Equal(wal ? 256 : 261, await CountRowsAsync(destination));
        }
        finally
        {
            await writer.CloseAsync();
            SqliteConnection.ClearAllPools();
            Cleanup(source);
            Cleanup(destination);
        }
    }

    [Theory]
    [InlineData(false, SqliteBackupMethod.Snapshot)]
    [InlineData(true, SqliteBackupMethod.Incremental)]
    public async Task BackupDatabaseAsync_ExplicitMethod_IsUsedWhateverTheJournalMode(bool wal, SqliteBackupMethod method)
    {
        string source = CreateDatabase(rowCount: 8, wal);
        string destination = NewDestination();
        try
        {
            using var sqlite = new SQLite();

            SqliteBackupResult result = await sqlite.BackupDatabaseAsync(source, destination, method);

            Assert.Equal(method, result.Method);
            Assert.Equal(8, await CountRowsAsync(destination));
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            Cleanup(source);
            Cleanup(destination);
        }
    }

    [Fact]
    public async Task BackupDatabase_WalDatabaseWrittenContinuously_CompletesWithAConsistentCopy()
    {
        // 32 MB in 256-page steps: a step-wise copy restarts after every commit and does not finish while the writer runs.
        string source = CreateDatabase(rowCount: 8192, wal: true);
        string destination = NewDestination();
        using var stopWriting = new CancellationTokenSource();
        int committed = 0;
        Task writer = Task.Run(() =>
        {
            using var connection = new SqliteConnection(SQLite.BuildConnectionString(source));
            connection.Open();
            using SqliteCommand insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO backup_contract(payload) VALUES(randomblob(64));";
            while (!stopWriting.IsCancellationRequested)
            {
                insert.ExecuteNonQuery();
                Interlocked.Increment(ref committed);
            }
        });
        using var sqlite = new SQLite();
        Task? backup = null;
        try
        {
            SpinWait.SpinUntil(() => Volatile.Read(ref committed) > 0, TimeSpan.FromSeconds(10));

            backup = Task.Run(() => sqlite.BackupDatabase(source, destination));
            Task finished = await Task.WhenAny(backup, Task.Delay(TimeSpan.FromSeconds(30)));
            stopWriting.Cancel();
            await writer;

            Assert.True(ReferenceEquals(backup, finished), "The backup did not complete while the database was written.");
            await backup;
            long copied = await CountRowsAsync(destination);
            Assert.InRange(copied, 8192, await CountRowsAsync(source));
            SqliteIntegrityCheckResult integrity = await sqlite.CheckIntegrityAsync(destination, fullCheck: true);
            Assert.True(integrity.IsHealthy);
        }
        finally
        {
            stopWriting.Cancel();
            await writer;
            if (backup != null)
            {
                // Once the writer stops, a backup that was still running can finish and release the files.
                await Task.WhenAny(backup, Task.Delay(TimeSpan.FromSeconds(30)));
            }
            SqliteConnection.ClearAllPools();
            Cleanup(source);
            Cleanup(destination);
        }
    }

    private static string NewDestination()
        => Path.Combine(Path.GetTempPath(), $"dbaclientx-backup-method-{Guid.NewGuid():N}.sqlite");

    private static string CreateDatabase(int rowCount, bool wal)
    {
        string path = Path.Combine(Path.GetTempPath(), $"dbaclientx-backup-method-{Guid.NewGuid():N}.sqlite");
        using var connection = new SqliteConnection(SQLite.BuildConnectionString(path));
        connection.Open();
        using SqliteCommand command = connection.CreateCommand();
        command.CommandText = wal ? "PRAGMA journal_mode = WAL;" : "PRAGMA journal_mode = DELETE;";
        command.ExecuteNonQuery();
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
        foreach (string suffix in new[] { string.Empty, "-wal", "-shm", "-journal" })
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

        public InlineProgress(Action<T> handler) => _handler = handler;

        public void Report(T value) => _handler(value);
    }
}
