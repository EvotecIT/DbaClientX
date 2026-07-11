using System.Diagnostics;
using Microsoft.Data.Sqlite;
using SQLitePCL;

namespace DBAClientX;

public partial class SQLite
{
    private const int MaximumBackupPagesPerStep = 4096;
    private const int MaximumBackupBusyTimeoutMs = 1000;

    /// <summary>
    /// Runs an SQLite integrity check on a dedicated thread so the provider's synchronous execution cannot block
    /// an asynchronous caller or scheduler.
    /// </summary>
    /// <param name="database">Source SQLite database path.</param>
    /// <param name="fullCheck">When true, uses <c>PRAGMA integrity_check</c>; otherwise uses <c>PRAGMA quick_check</c>.</param>
    /// <param name="maxIssues">Maximum number of integrity issues returned by SQLite.</param>
    /// <param name="busyTimeoutMs">Optional busy timeout in milliseconds.</param>
    /// <param name="cancellationToken">Token used to interrupt the native SQLite command.</param>
    /// <returns>A task containing the integrity result.</returns>
    public virtual Task<SqliteIntegrityCheckResult> CheckIntegrityAsync(
        string database,
        bool fullCheck = false,
        int maxIssues = 10,
        int? busyTimeoutMs = null,
        CancellationToken cancellationToken = default)
    {
        ValidateDatabasePath(database);
        if (maxIssues <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxIssues), "Maximum issue count must be positive.");
        }
        EnsureNoActiveTransaction();

        return RunDedicatedMaintenanceAsync(
            () => CheckIntegrityCore(database, fullCheck, maxIssues, busyTimeoutMs, cancellationToken),
            cancellationToken);
    }

    /// <summary>
    /// Copies an SQLite database incrementally on a dedicated thread using SQLite's online backup API.
    /// </summary>
    /// <param name="sourceDatabase">Source SQLite database path.</param>
    /// <param name="destinationDatabase">Destination SQLite database path.</param>
    /// <param name="options">Backup behavior options.</param>
    /// <param name="progress">Optional page-based progress observer.</param>
    /// <param name="cancellationToken">Token used to stop between backup steps and interrupt native work.</param>
    /// <returns>A task containing the completed backup details.</returns>
    public virtual Task<SqliteBackupResult> BackupDatabaseIncrementalAsync(
        string sourceDatabase,
        string destinationDatabase,
        SqliteBackupOptions? options = null,
        IProgress<SqliteBackupProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        ValidateDatabasePath(sourceDatabase);
        ValidateDatabasePath(destinationDatabase);
        EnsureNoActiveTransaction();
        SqliteBackupOptions effectiveOptions = SnapshotBackupOptions(options);
        ValidateBackupOptions(effectiveOptions);
        int backupBusyTimeoutMs = ResolveBackupBusyTimeoutMs(effectiveOptions.BusyTimeoutMs, BusyTimeoutMs);

        return RunDedicatedMaintenanceAsync(
            () => BackupDatabaseIncrementalCore(
                sourceDatabase,
                destinationDatabase,
                effectiveOptions,
                backupBusyTimeoutMs,
                progress,
                cancellationToken),
            cancellationToken);
    }

    private SqliteIntegrityCheckResult CheckIntegrityCore(
        string database,
        bool fullCheck,
        int maxIssues,
        int? busyTimeoutMs,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!File.Exists(database))
        {
            throw new FileNotFoundException($"SQLite database file does not exist: {database}", database);
        }

        var stopwatch = Stopwatch.StartNew();
        using var connection = new SqliteConnection(BuildOperationalConnectionString(database, readOnly: true));
        connection.Open();
        ApplyBusyTimeout(connection, busyTimeoutMs);
        using CancellationTokenRegistration registration = cancellationToken.Register(
            static state => raw.sqlite3_interrupt(((SqliteConnection)state!).Handle),
            connection);
        using var command = connection.CreateCommand();
        command.CommandText = fullCheck
            ? $"PRAGMA integrity_check({maxIssues});"
            : $"PRAGMA quick_check({maxIssues});";
        if (CommandTimeout > 0)
        {
            command.CommandTimeout = CommandTimeout;
        }

        var issues = new List<string>();
        try
        {
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                cancellationToken.ThrowIfCancellationRequested();
                string value = reader.IsDBNull(0) ? string.Empty : reader.GetString(0).Trim();
                if (value.Length > 0 && !string.Equals(value, "ok", StringComparison.OrdinalIgnoreCase))
                {
                    issues.Add(value);
                }
            }
        }
        catch (SqliteException) when (cancellationToken.IsCancellationRequested)
        {
            throw new OperationCanceledException(cancellationToken);
        }

        stopwatch.Stop();
        return new SqliteIntegrityCheckResult
        {
            IsHealthy = issues.Count == 0,
            IsFullCheck = fullCheck,
            Issues = issues,
            Elapsed = stopwatch.Elapsed
        };
    }

    private SqliteBackupResult BackupDatabaseIncrementalCore(
        string sourceDatabase,
        string destinationDatabase,
        SqliteBackupOptions options,
        int backupBusyTimeoutMs,
        IProgress<SqliteBackupProgress>? progress,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        string sourcePath = Path.GetFullPath(sourceDatabase);
        string destinationPath = Path.GetFullPath(destinationDatabase);
        if (string.Equals(sourcePath, destinationPath, StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException("Source and destination database paths must be different.", nameof(destinationDatabase));
        }
        if (!File.Exists(sourcePath))
        {
            throw new FileNotFoundException($"SQLite database file does not exist: {sourcePath}", sourcePath);
        }

        string? destinationDirectory = Path.GetDirectoryName(destinationPath);
        if (!string.IsNullOrWhiteSpace(destinationDirectory))
        {
            Directory.CreateDirectory(destinationDirectory);
        }
        bool destinationExisted = File.Exists(destinationPath);
        if (destinationExisted)
        {
            if (!options.OverwriteDestination)
            {
                throw new IOException($"SQLite backup destination already exists: {destinationPath}");
            }
        }

        string workingPath = destinationExisted
            ? $"{destinationPath}.{Guid.NewGuid():N}.partial"
            : destinationPath;

        var stopwatch = Stopwatch.StartNew();
        bool completed = false;
        int totalPages = 0;
        try
        {
            {
                using var source = new SqliteConnection(BuildOperationalConnectionString(sourcePath, readOnly: true));
                using var destination = new SqliteConnection(BuildOperationalConnectionString(workingPath));
                source.Open();
                destination.Open();
                ApplyBusyTimeout(source, backupBusyTimeoutMs);
                ApplyBusyTimeout(destination, backupBusyTimeoutMs);
                using CancellationTokenRegistration sourceRegistration = cancellationToken.Register(
                    static state => raw.sqlite3_interrupt(((SqliteConnection)state!).Handle),
                    source);
                using CancellationTokenRegistration destinationRegistration = cancellationToken.Register(
                    static state => raw.sqlite3_interrupt(((SqliteConnection)state!).Handle),
                    destination);

                sqlite3_backup? backup = raw.sqlite3_backup_init(destination.Handle, "main", source.Handle, "main");
                if (backup == null || backup.IsInvalid)
                {
                    string message = raw.sqlite3_errmsg(destination.Handle).utf8_to_string();
                    throw new DbaQueryExecutionException("Failed to initialize SQLite online backup.", "SQLite online backup", new InvalidOperationException(message));
                }

                int resultCode = raw.SQLITE_OK;
                int remainingPages = 0;
                TimeSpan cumulativeBusyDuration = TimeSpan.Zero;
                Exception? backupFailure = null;
                try
                {
                    while (resultCode != raw.SQLITE_DONE)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        var stepStopwatch = Stopwatch.StartNew();
                        resultCode = raw.sqlite3_backup_step(backup, options.PagesPerStep);
                        stepStopwatch.Stop();
                        totalPages = raw.sqlite3_backup_pagecount(backup);
                        remainingPages = raw.sqlite3_backup_remaining(backup);
                        ReportBackupProgress(progress, totalPages, remainingPages, stopwatch.Elapsed);

                        if (resultCode == raw.SQLITE_DONE)
                        {
                            break;
                        }
                        if (resultCode != raw.SQLITE_OK && resultCode != raw.SQLITE_BUSY && resultCode != raw.SQLITE_LOCKED)
                        {
                            string message = raw.sqlite3_errmsg(destination.Handle).utf8_to_string();
                            throw new DbaQueryExecutionException(
                                $"SQLite online backup failed with result code {resultCode}.",
                                "SQLite online backup",
                                new InvalidOperationException(message));
                        }

                        bool isBusy = resultCode == raw.SQLITE_BUSY || resultCode == raw.SQLITE_LOCKED;
                        if (isBusy)
                        {
                            cumulativeBusyDuration += stepStopwatch.Elapsed;
                            ThrowIfBusyRetryTimeoutExceeded(cumulativeBusyDuration, options.BusyRetryTimeout);
                        }

                        TimeSpan delay = isBusy
                            ? options.BusyRetryDelay
                            : options.StepDelay;
                        if (isBusy && delay > TimeSpan.Zero)
                        {
                            var delayStopwatch = Stopwatch.StartNew();
                            WaitWithCancellation(delay, cancellationToken);
                            delayStopwatch.Stop();
                            cumulativeBusyDuration += delayStopwatch.Elapsed;
                            ThrowIfBusyRetryTimeoutExceeded(cumulativeBusyDuration, options.BusyRetryTimeout);
                        }
                        else
                        {
                            WaitWithCancellation(delay, cancellationToken);
                        }
                    }
                }
                catch (Exception exception)
                {
                    backupFailure = exception;
                    throw;
                }
                finally
                {
                    int finishCode = raw.sqlite3_backup_finish(backup);
                    if (backupFailure == null && resultCode == raw.SQLITE_DONE && finishCode != raw.SQLITE_OK)
                    {
                        string message = raw.sqlite3_errmsg(destination.Handle).utf8_to_string();
                        throw new DbaQueryExecutionException(
                            $"SQLite online backup finalization failed with result code {finishCode}.",
                            "SQLite online backup",
                            new InvalidOperationException(message));
                    }
                }
            }

            if (destinationExisted)
            {
                ReplaceBackupDestination(workingPath, destinationPath);
            }

            stopwatch.Stop();
            completed = true;
            ReportBackupProgress(progress, totalPages, 0, stopwatch.Elapsed);
            return new SqliteBackupResult
            {
                SourceDatabase = sourcePath,
                DestinationDatabase = destinationPath,
                CopiedPages = totalPages,
                DestinationLengthBytes = new FileInfo(destinationPath).Length,
                Elapsed = stopwatch.Elapsed
            };
        }
        catch (SqliteException) when (cancellationToken.IsCancellationRequested)
        {
            throw new OperationCanceledException(cancellationToken);
        }
        finally
        {
            if (!completed && options.DeleteDestinationOnFailure)
            {
                TryDeleteBackupDestination(workingPath);
            }
        }
    }

    private static Task<T> RunDedicatedMaintenanceAsync<T>(Func<T> operation, CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled<T>(cancellationToken);
        }

        return Task.Factory.StartNew(
            operation,
            CancellationToken.None,
            TaskCreationOptions.DenyChildAttach | TaskCreationOptions.LongRunning,
            TaskScheduler.Default);
    }

    private static void ValidateBackupOptions(SqliteBackupOptions options)
    {
        if (options.PagesPerStep <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options.PagesPerStep), "Pages per step must be positive.");
        }
        if (options.PagesPerStep > MaximumBackupPagesPerStep)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options.PagesPerStep),
                $"Pages per step cannot exceed {MaximumBackupPagesPerStep} so cancellation remains responsive.");
        }
        if (options.StepDelay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(options.StepDelay), "Step delay cannot be negative.");
        }
        if (options.BusyRetryDelay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(options.BusyRetryDelay), "Busy retry delay cannot be negative.");
        }
        if (options.BusyRetryTimeout < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(options.BusyRetryTimeout), "Busy retry timeout cannot be negative.");
        }
        if (options.BusyTimeoutMs < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options.BusyTimeoutMs), "Busy timeout cannot be negative.");
        }
    }

    internal static int ResolveBackupBusyTimeoutMs(int? requestedBusyTimeoutMs, int instanceBusyTimeoutMs)
        => Math.Min(requestedBusyTimeoutMs ?? instanceBusyTimeoutMs, MaximumBackupBusyTimeoutMs);

    internal static SqliteBackupOptions SnapshotBackupOptions(SqliteBackupOptions? options)
    {
        SqliteBackupOptions source = options ?? new SqliteBackupOptions();
        return new SqliteBackupOptions
        {
            PagesPerStep = source.PagesPerStep,
            StepDelay = source.StepDelay,
            BusyRetryDelay = source.BusyRetryDelay,
            BusyRetryTimeout = source.BusyRetryTimeout,
            BusyTimeoutMs = source.BusyTimeoutMs,
            OverwriteDestination = source.OverwriteDestination,
            DeleteDestinationOnFailure = source.DeleteDestinationOnFailure
        };
    }

    private static void ReplaceBackupDestination(string workingPath, string destinationPath)
    {
        var quarantinedSidecars = new List<KeyValuePair<string, string>>();
        bool replaced = false;
        try
        {
            foreach (string suffix in new[] { "-wal", "-shm", "-journal" })
            {
                string sidecarPath = destinationPath + suffix;
                if (!File.Exists(sidecarPath))
                {
                    continue;
                }

                string quarantinePath = $"{destinationPath}.{Guid.NewGuid():N}.stale{suffix}";
                File.Move(sidecarPath, quarantinePath);
                quarantinedSidecars.Add(new KeyValuePair<string, string>(sidecarPath, quarantinePath));
            }

            File.Replace(workingPath, destinationPath, null, ignoreMetadataErrors: true);
            replaced = true;
        }
        finally
        {
            foreach (KeyValuePair<string, string> sidecar in quarantinedSidecars)
            {
                try
                {
                    if (replaced)
                    {
                        File.Delete(sidecar.Value);
                    }
                    else if (!File.Exists(sidecar.Key))
                    {
                        File.Move(sidecar.Value, sidecar.Key);
                    }
                }
                catch
                {
                    // A quarantined sidecar cannot affect the promoted backup; failed restoration is surfaced by
                    // the original replacement exception while cleanup remains best effort.
                }
            }
        }
    }

    private static void ThrowIfBusyRetryTimeoutExceeded(TimeSpan elapsed, TimeSpan timeout)
    {
        if (timeout > TimeSpan.Zero && elapsed >= timeout)
        {
            throw new TimeoutException($"SQLite online backup cumulative busy or locked time reached {elapsed:g}.");
        }
    }

    private static void ReportBackupProgress(
        IProgress<SqliteBackupProgress>? progress,
        int totalPages,
        int remainingPages,
        TimeSpan elapsed)
    {
        if (progress == null)
        {
            return;
        }

        int copiedPages = Math.Max(0, totalPages - remainingPages);
        double percentage = totalPages > 0 ? copiedPages * 100d / totalPages : 0d;
        progress.Report(new SqliteBackupProgress
        {
            TotalPages = totalPages,
            CopiedPages = copiedPages,
            RemainingPages = Math.Max(0, remainingPages),
            PercentComplete = percentage,
            Elapsed = elapsed
        });
    }

    private static void WaitWithCancellation(TimeSpan delay, CancellationToken cancellationToken)
    {
        if (delay <= TimeSpan.Zero)
        {
            return;
        }
        if (cancellationToken.WaitHandle.WaitOne(delay))
        {
            cancellationToken.ThrowIfCancellationRequested();
        }
    }

    private static void TryDeleteBackupDestination(string path)
    {
        try
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
        catch
        {
            // The caller receives the original backup failure; cleanup is best effort.
        }
    }
}
