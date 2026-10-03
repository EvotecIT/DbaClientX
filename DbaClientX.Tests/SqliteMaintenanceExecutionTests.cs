using DBAClientX;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

public sealed class SqliteMaintenanceExecutionTests
{
    [Fact]
    public async Task BackupDatabase_ExistingDestinationRequiresExplicitOverwrite()
    {
        string source = CreateDatabase(rowCount: 2);
        string destination = CreateDatabase(rowCount: 1);
        try
        {
            using var sqlite = new SQLite();

            Assert.Throws<IOException>(() => sqlite.BackupDatabase(source, destination));
            Assert.Equal(1, await CountRowsAsync(destination));
        }
        finally
        {
            Cleanup(source);
            Cleanup(destination);
        }
    }

    [Fact]
    public async Task BackupDatabase_ExplicitOverwriteAtomicallyReplacesDestination()
    {
        string source = CreateDatabase(rowCount: 2);
        string destination = CreateDatabase(rowCount: 1);
        try
        {
            using var sqlite = new SQLite();

            sqlite.BackupDatabase(source, destination, overwriteDestination: true);

            Assert.Equal(2, await CountRowsAsync(destination));
        }
        finally
        {
            Cleanup(source);
            Cleanup(destination);
        }
    }

    [Fact]
    public void BackupDatabase_OperationalFailureUsesSanitizedLegacyExceptionContract()
    {
        string source = CreateDatabase(rowCount: 1);
        string blockingFile = Path.Combine(Path.GetTempPath(), $"dbaclientx-backup-parent-{Guid.NewGuid():N}");
        string destination = Path.Combine(blockingFile, "backup.sqlite");
        File.WriteAllText(blockingFile, "not a directory");
        try
        {
            using var sqlite = new SQLite();

            var exception = Assert.Throws<DbaQueryExecutionException>(() =>
                sqlite.BackupDatabase(source, destination));

            Assert.Contains("Failed to back up SQLite database", exception.Message, StringComparison.Ordinal);
            Assert.Equal(typeof(IOException).FullName, exception.ProviderExceptionType);
            Assert.DoesNotContain(blockingFile, exception.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            Cleanup(source);
            Cleanup(destination);
            Cleanup(blockingFile);
        }
    }

    [Fact]
    public void BackupDatabase_RejectsSameSourceAndDestination()
    {
        string source = CreateDatabase();
        try
        {
            using var sqlite = new SQLite();

            Assert.Throws<ArgumentException>(() => sqlite.BackupDatabase(source, source, overwriteDestination: true));
        }
        finally
        {
            Cleanup(source);
        }
    }

    [Fact]
    public void BackupDatabase_RejectsCaseOnlyAliasOnCaseInsensitiveFileSystem()
    {
        string directory = Path.Combine(Path.GetTempPath(), $"dbaclientx-case-alias-{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        string source = Path.Combine(directory, "app.db");
        string destination = Path.Combine(directory, "App.db");
        try
        {
            CreateDatabase(source, rowCount: 1);
            Assert.SkipUnless(File.Exists(destination), "The temporary filesystem is case-sensitive.");

            Assert.True(SQLite.AreSameBackupPath(source, destination));
        }
        finally
        {
            Cleanup(source);
            Cleanup(destination);
            if (Directory.Exists(directory)) Directory.Delete(directory);
        }
    }

    [Fact]
    public async Task BackupDatabase_AllowsCaseDistinctPathsOnCaseSensitiveFileSystems()
    {
        string directory = Path.Combine(Path.GetTempPath(), $"dbaclientx-case-{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        string source = Path.Combine(directory, "app.db");
        string destination = Path.Combine(directory, "App.db");
        try
        {
            CreateDatabase(source, rowCount: 2);
            Assert.SkipWhen(File.Exists(destination), "The temporary filesystem is case-insensitive.");
            CreateDatabase(destination, rowCount: 1);
            using var sqlite = new SQLite();

            sqlite.BackupDatabase(source, destination, overwriteDestination: true);

            Assert.Equal(2, await CountRowsAsync(destination));
        }
        finally
        {
            Cleanup(source);
            Cleanup(destination);
            if (Directory.Exists(directory)) Directory.Delete(directory);
        }
    }

    [Fact]
    public void BackupDatabase_RejectsZeroBusyTimeoutInsteadOfSelectingUnboundedRetries()
    {
        string source = CreateDatabase();
        string destination = Path.Combine(Path.GetTempPath(), $"dbaclientx-zero-timeout-{Guid.NewGuid():N}.sqlite");
        try
        {
            using var sqlite = new SQLite();

            var exception = Assert.Throws<ArgumentOutOfRangeException>(() =>
                sqlite.BackupDatabase(source, destination, busyTimeoutMs: 0));

            Assert.Equal("busyTimeoutMs", exception.ParamName);
            Assert.False(File.Exists(destination));
        }
        finally
        {
            Cleanup(source);
            Cleanup(destination);
        }
    }

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
    public async Task CheckIntegrityAsync_OperationalFailure_IsSanitized()
    {
        string database = Path.Combine(Path.GetTempPath(), $"dbaclientx-invalid-{Guid.NewGuid():N}.sqlite");
        await File.WriteAllTextAsync(database, "not a sqlite database; server=secret;password=hidden");
        try
        {
            using var sqlite = new SQLite();

            DbaQueryExecutionException exception = await Assert.ThrowsAsync<DbaQueryExecutionException>(() =>
                sqlite.CheckIntegrityAsync(database, fullCheck: true));

            Assert.Contains("integrity", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.NotNull(exception.ProviderErrorCode);
            Assert.DoesNotContain("password=hidden", exception.ToString(), StringComparison.OrdinalIgnoreCase);
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

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task BackupDatabase_WhileAnotherConnectionWrites_SnapshotCopiesTheStartState(bool snapshot)
    {
        string source = CreateDatabase(rowCount: 256);
        string destination = Path.Combine(Path.GetTempPath(), $"dbaclientx-concurrent-{Guid.NewGuid():N}.sqlite");
        await using var writer = new SqliteConnection(SQLite.BuildConnectionString(source));
        try
        {
            await writer.OpenAsync();
            await using (SqliteCommand wal = writer.CreateCommand())
            {
                wal.CommandText = "PRAGMA journal_mode = WAL;";
                await wal.ExecuteNonQueryAsync();
            }

            // Another connection commits a row after each of the first steps, as a service writing during the backup.
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
            var options = new SqliteBackupOptions { PagesPerStep = 16 };

            SqliteBackupResult result = snapshot
                ? await sqlite.BackupDatabaseSnapshotAsync(source, destination, options, progress)
                : await sqlite.BackupDatabaseIncrementalAsync(source, destination, options, progress);

            // The snapshot copy holds the rows of its start; the step-wise copy started over after each write.
            Assert.Equal(5, writes);
            Assert.Equal(snapshot ? 256 : 261, await CountRowsAsync(destination));
            Assert.Equal(261, await CountRowsAsync(source));
            Assert.True(result.CopiedPages > 0);
        }
        finally
        {
            await writer.CloseAsync();
            SqliteConnection.ClearAllPools();
            Cleanup(source);
            Cleanup(destination);
        }
    }

    [Fact]
    public async Task BackupDatabaseSnapshotAsync_CanceledBackup_DeletesDestinationAndReleasesTheSnapshot()
    {
        string source = CreateDatabase(rowCount: 256);
        string destination = Path.Combine(Path.GetTempPath(), $"dbaclientx-snapshot-canceled-{Guid.NewGuid():N}.sqlite");
        await using var writer = new SqliteConnection(SQLite.BuildConnectionString(source));
        using var cancellationSource = new CancellationTokenSource();
        try
        {
            await writer.OpenAsync();
            await ExecuteAsync(writer, "PRAGMA journal_mode = WAL;");
            using var sqlite = new SQLite();
            var progress = new InlineProgress<SqliteBackupProgress>(value =>
            {
                if (value.CopiedPages > 0)
                {
                    cancellationSource.Cancel();
                }
            });

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sqlite.BackupDatabaseSnapshotAsync(
                source,
                destination,
                new SqliteBackupOptions { PagesPerStep = 8 },
                progress,
                cancellationSource.Token));

            // A TRUNCATE checkpoint completes only when no reader holds an older snapshot.
            Assert.False(File.Exists(destination));
            await ExecuteAsync(writer, "INSERT INTO backup_contract(payload) VALUES(randomblob(16));");
            await using SqliteCommand checkpoint = writer.CreateCommand();
            checkpoint.CommandText = "PRAGMA wal_checkpoint(TRUNCATE);";
            await using SqliteDataReader reader = await checkpoint.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal(0L, reader.GetInt64(0));
        }
        finally
        {
            await writer.CloseAsync();
            SqliteConnection.ClearAllPools();
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
            File.WriteAllText(destination + "-journal", "stale-journal");
            using var sqlite = new SQLite();

            await sqlite.BackupDatabaseIncrementalAsync(
                source,
                destination,
                new SqliteBackupOptions { OverwriteDestination = true });

            Assert.False(File.Exists(destination + "-wal"));
            Assert.False(File.Exists(destination + "-shm"));
            Assert.False(File.Exists(destination + "-journal"));
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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    [InlineData(null)]
    public async Task BackupDatabase_LockedSource_EnforcesExplicitBusyDeadline(bool? snapshot)
    {
        string source = CreateDatabase();
        string destination = Path.Combine(Path.GetTempPath(), $"dbaclientx-busy-deadline-{Guid.NewGuid():N}.sqlite");
        await using var lockConnection = new SqliteConnection(SQLite.BuildConnectionString(source));
        try
        {
            await lockConnection.OpenAsync();
            await using SqliteCommand lockCommand = lockConnection.CreateCommand();
            lockCommand.CommandText = "BEGIN EXCLUSIVE;";
            await lockCommand.ExecuteNonQueryAsync();
            using var sqlite = new SQLite();
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            var options = new SqliteBackupOptions
            {
                BusyRetryDelay = TimeSpan.FromMilliseconds(20),
                BusyRetryTimeout = TimeSpan.FromMilliseconds(100)
            };

            if (snapshot.HasValue) {
                await Assert.ThrowsAsync<TimeoutException>(() => snapshot.Value
                    ? sqlite.BackupDatabaseSnapshotAsync(source, destination, options)
                    : sqlite.BackupDatabaseIncrementalAsync(source, destination, options));
            } else {
                var failure = await Assert.ThrowsAsync<DbaQueryExecutionException>(() => sqlite.BackupDatabaseAsync(source, destination, options: options));
                Assert.IsType<TimeoutException>(failure.InnerException);
            }
            Assert.False(File.Exists(destination));

            stopwatch.Stop();
            Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(1), $"Busy deadline took {stopwatch.Elapsed}.");
        }
        finally
        {
            if (lockConnection.State == System.Data.ConnectionState.Open)
            {
                await using SqliteCommand rollback = lockConnection.CreateCommand();
                rollback.CommandText = "ROLLBACK;";
                await rollback.ExecuteNonQueryAsync();
            }
            await lockConnection.CloseAsync();
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
        }
        finally
        {
            Cleanup(source);
            Cleanup(destination);
        }
    }

    [Fact]
    public void SnapshotBackupOptions_CallerMutation_DoesNotChangeValidatedSnapshot()
    {
        var options = new SqliteBackupOptions
        {
            PagesPerStep = 64,
            StepDelay = TimeSpan.FromMilliseconds(10),
            BusyRetryDelay = TimeSpan.FromMilliseconds(20),
            BusyRetryTimeout = TimeSpan.FromSeconds(3),
            OverwriteDestination = true,
            DeleteDestinationOnFailure = false
        };
        var method = typeof(SQLite).GetMethod(
            "SnapshotBackupOptions",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;

        var snapshot = Assert.IsType<SqliteBackupOptions>(method.Invoke(null, new object?[] { options }));
        options.PagesPerStep = 4096;
        options.OverwriteDestination = false;

        Assert.NotSame(options, snapshot);
        Assert.Equal(64, snapshot.PagesPerStep);
        Assert.Equal(TimeSpan.FromMilliseconds(10), snapshot.StepDelay);
        Assert.Equal(TimeSpan.FromMilliseconds(20), snapshot.BusyRetryDelay);
        Assert.Equal(TimeSpan.FromSeconds(3), snapshot.BusyRetryTimeout);
        Assert.True(snapshot.OverwriteDestination);
        Assert.False(snapshot.DeleteDestinationOnFailure);
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
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sqlite.BackupDatabaseSnapshotAsync(
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
        return CreateDatabase(path, rowCount);
    }

    private static string CreateDatabase(string path, int rowCount)
    {
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

    private static async Task ExecuteAsync(SqliteConnection connection, string sql)
    {
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
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
