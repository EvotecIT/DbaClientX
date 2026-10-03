using System.Diagnostics;
using Microsoft.Data.Sqlite;
using SQLitePCL;

namespace DBAClientX;

public partial class SQLite
{
    private const int MaximumBackupPagesPerStep = 4096;

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
    /// Copies an SQLite database incrementally on a dedicated thread using SQLite's online backup API, releasing the
    /// source between steps.
    /// </summary>
    /// <param name="sourceDatabase">Source SQLite database path.</param>
    /// <param name="destinationDatabase">Destination SQLite database path.</param>
    /// <param name="options">Backup behavior options.</param>
    /// <param name="progress">Optional page-based progress observer.</param>
    /// <param name="cancellationToken">Token used to stop between backup steps and interrupt native work.</param>
    /// <returns>A task containing the completed backup details.</returns>
    /// <remarks>
    /// Between steps the source is unlocked, so a rollback-journal writer waits for one step at most (a WAL writer not
    /// at all), but SQLite restarts the copy from the first page whenever another connection changes the source: on a
    /// database that is written to continuously it never completes (on a 14.55 GB WAL database written 6 times a second
    /// it restarted 1,080 times in 3 minutes and never passed 0.6%). Use it for a database in rollback-journal mode,
    /// where holding a snapshot would block writers, when writes pause long enough for a whole copy, or for a database
    /// that is not being written. For a WAL database that is written to while it is backed up, use
    /// <see cref="BackupDatabaseSnapshotAsync"/>.
    /// </remarks>
    public virtual Task<SqliteBackupResult> BackupDatabaseIncrementalAsync(
        string sourceDatabase,
        string destinationDatabase,
        SqliteBackupOptions? options = null,
        IProgress<SqliteBackupProgress>? progress = null,
        CancellationToken cancellationToken = default)
        => StartBackup(sourceDatabase, destinationDatabase, SqliteBackupMethod.Incremental, options, progress, cancellationToken);

    /// <summary>
    /// Copies a consistent snapshot of an SQLite database on a dedicated thread using SQLite's online backup API: the
    /// source connection holds one read transaction across every step, so the copy completes while other connections
    /// write, and it holds the database exactly as it was when the backup started.
    /// </summary>
    /// <param name="sourceDatabase">Source SQLite database path.</param>
    /// <param name="destinationDatabase">Destination SQLite database path.</param>
    /// <param name="options">Backup behavior options; <see cref="SqliteBackupOptions.PagesPerStep"/> sets how often
    /// progress is reported and cancellation is checked.</param>
    /// <param name="progress">Optional page-based progress observer.</param>
    /// <param name="cancellationToken">Token used to stop between backup steps and interrupt native work.</param>
    /// <returns>A task containing the completed backup details.</returns>
    /// <remarks>
    /// <para>Changes committed after the backup starts are not in the copy, and they do not restart it. On a 14.55 GB
    /// WAL database written 6 times a second, the copy took 35 to 69 s over six runs on a shared machine (the
    /// step-wise <see cref="BackupDatabaseIncrementalAsync"/> never completed),
    /// the concurrent writes kept their latency, and <c>PRAGMA integrity_check</c> and the row counts of the copy
    /// matched the snapshot.</para>
    /// <para>Holding the snapshot has costs. In WAL mode a checkpoint cannot move pages written after the snapshot into
    /// the database file until the backup ends, so the WAL grows by what is written meanwhile. A <c>FULL</c>,
    /// <c>RESTART</c> or <c>TRUNCATE</c> checkpoint run meanwhile waits for the backup for the busy timeout of the
    /// connection that runs it, and writers wait behind that checkpoint; <c>PASSIVE</c> checkpoints (the automatic ones)
    /// do not wait. In rollback-journal mode the snapshot is a shared lock: writers cannot commit until the backup ends
    /// (they fail once their busy timeout ends), and readers wait too once a writer is pending. A rollback-journal
    /// database that is written continuously cannot be backed up by either method; switch it to WAL.</para>
    /// </remarks>
    public virtual Task<SqliteBackupResult> BackupDatabaseSnapshotAsync(
        string sourceDatabase,
        string destinationDatabase,
        SqliteBackupOptions? options = null,
        IProgress<SqliteBackupProgress>? progress = null,
        CancellationToken cancellationToken = default)
        => StartBackup(sourceDatabase, destinationDatabase, SqliteBackupMethod.Snapshot, options, progress, cancellationToken);

    private Task<SqliteBackupResult> StartBackup(
        string sourceDatabase,
        string destinationDatabase,
        SqliteBackupMethod method,
        SqliteBackupOptions? options,
        IProgress<SqliteBackupProgress>? progress,
        CancellationToken cancellationToken)
    {
        ValidateDatabasePath(sourceDatabase);
        ValidateDatabasePath(destinationDatabase);
        ValidateBackupMethod(method);
        EnsureNoActiveTransaction();
        SqliteBackupOptions effectiveOptions = SnapshotBackupOptions(options);
        ValidateBackupOptions(effectiveOptions);

        return RunDedicatedMaintenanceAsync(
            () => BackupDatabaseCore(
                sourceDatabase,
                destinationDatabase,
                method,
                effectiveOptions,
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
        if (!File.Exists(GetSQLiteFileSystemPath(database)))
        {
            throw new FileNotFoundException($"SQLite database file does not exist: {database}", database);
        }

        var stopwatch = Stopwatch.StartNew();
        try
        {
            using var connection = CreateConfiguredConnection(BuildOperationalConnectionString(database, readOnly: true));
            OpenConnectionWithDiagnostics(connection);
            ApplyBusyTimeout(connection, busyTimeoutMs);
            ApplyConnectionConfiguration(connection);
            using CancellationTokenRegistration registration = RegisterStatementInterrupt(connection, cancellationToken);
            using var command = connection.CreateCommand();
            command.CommandText = fullCheck
                ? $"PRAGMA integrity_check({maxIssues});"
                : $"PRAGMA quick_check({maxIssues});";
            ApplyCommandTimeout(command);

            var issues = new List<string>();
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

            stopwatch.Stop();
            return new SqliteIntegrityCheckResult
            {
                IsHealthy = issues.Count == 0,
                IsFullCheck = fullCheck,
                Issues = issues,
                Elapsed = stopwatch.Elapsed
            };
        }
        catch (SqliteException ex) when (
            cancellationToken.IsCancellationRequested &&
            IsProviderCancellationException(ex))
        {
            throw CreateCallerCancellationException(ex, cancellationToken);
        }
        catch (SqliteException ex)
        {
            throw CreateQueryExecutionException(
                "Failed to check SQLite database integrity.",
                fullCheck ? "PRAGMA integrity_check" : "PRAGMA quick_check",
                ex);
        }
    }

    private SqliteBackupResult BackupDatabaseCore(
        string sourceDatabase,
        string destinationDatabase,
        SqliteBackupMethod method,
        SqliteBackupOptions options,
        IProgress<SqliteBackupProgress>? progress,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        string sourcePath = GetSQLiteFileSystemPath(sourceDatabase);
        string destinationPath = GetSQLiteFileSystemPath(destinationDatabase);
        if (AreSameBackupPath(sourcePath, destinationPath))
        {
            throw new ArgumentException("Source and destination database paths must be different.", nameof(destinationDatabase));
        }
        if (!File.Exists(sourcePath))
        {
            throw new FileNotFoundException($"SQLite database file does not exist: {sourcePath}", sourcePath);
        }
        SqliteBackupMethod effectiveMethod = method;

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
                OpenConnectionWithDiagnostics(source);
                OpenConnectionWithDiagnostics(destination);
                using CancellationTokenRegistration sourceRegistration = RegisterStatementInterrupt(source, cancellationToken);
                using CancellationTokenRegistration destinationRegistration = RegisterStatementInterrupt(destination, cancellationToken);
                TimeSpan cumulativeBusyDuration = TimeSpan.Zero;
                effectiveMethod = ResolveBackupMethod(method, source, options, ref cumulativeBusyDuration, cancellationToken);
                using SourceSnapshot? sourceSnapshot = effectiveMethod == SqliteBackupMethod.Snapshot
                    ? BeginSourceSnapshot(source, options, ref cumulativeBusyDuration, cancellationToken)
                    : null;

                sqlite3_backup? backup = raw.sqlite3_backup_init(destination.Handle, "main", source.Handle, "main");
                if (backup == null || backup.IsInvalid)
                {
                    string message = raw.sqlite3_errmsg(destination.Handle).utf8_to_string();
                    int initializationCode = raw.sqlite3_errcode(destination.Handle);
                    throw CreateBackupProviderException(
                        "Failed to initialize SQLite online backup.",
                        new InvalidOperationException(message),
                        initializationCode);
                }

                int resultCode = raw.SQLITE_OK;
                int remainingPages = 0;
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
                        cancellationToken.ThrowIfCancellationRequested();

                        if (resultCode == raw.SQLITE_DONE)
                        {
                            break;
                        }
                        bool isBusy = IsBusyResult(resultCode);
                        if (resultCode != raw.SQLITE_OK && !isBusy)
                        {
                            string message = raw.sqlite3_errmsg(destination.Handle).utf8_to_string();
                            throw CreateBackupProviderException(
                                $"SQLite online backup failed with result code {resultCode}.",
                                new InvalidOperationException(message),
                                resultCode);
                        }

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
                        throw CreateBackupProviderException(
                            $"SQLite online backup finalization failed with result code {finishCode}.",
                            new InvalidOperationException(message),
                            finishCode);
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
                Elapsed = stopwatch.Elapsed,
                Method = effectiveMethod
            };
        }
        catch (SqliteException ex) when (
            cancellationToken.IsCancellationRequested &&
            IsProviderCancellationException(ex))
        {
            throw CreateCallerCancellationException(ex, cancellationToken);
        }
        finally
        {
            if (!completed && options.DeleteDestinationOnFailure)
            {
                TryDeleteBackupDestination(workingPath);
            }
        }
    }

    internal static bool AreSameBackupPath(string sourcePath, string destinationPath)
    {
        // Extended spellings of the same name can be rejected without touching a remote share.
        if (string.Equals(SQLiteFilePath.NormalizeWindowsAlias(sourcePath),
            SQLiteFilePath.NormalizeWindowsAlias(destinationPath), StringComparison.Ordinal)) return true;
        sourcePath = SQLiteFilePath.ResolveAliases(sourcePath);
        destinationPath = SQLiteFilePath.ResolveAliases(destinationPath);
        if (string.Equals(sourcePath, destinationPath, StringComparison.Ordinal)) return true;
        if (!string.Equals(sourcePath, destinationPath, StringComparison.OrdinalIgnoreCase)) return false;

        // Case-only names can be separate files on a case-sensitive filesystem. If the destination
        // does not resolve, it is safe to create it. If it resolves, inspect the directory entries:
        // a case-insensitive filesystem exposes only one exact spelling for both aliases.
        // Canonical aliases are for identity comparison. Managed filesystem operations still
        // need extended filenames in Framework hosts when the ordinary spelling is long.
        string sourceFilePath = GetSQLiteFileSystemPath(sourcePath);
        string destinationFilePath = GetSQLiteFileSystemPath(destinationPath);
        if (!File.Exists(sourceFilePath) || !File.Exists(destinationFilePath)) return false;
        string? sourceDirectory = Path.GetDirectoryName(sourcePath);
        string? destinationDirectory = Path.GetDirectoryName(destinationPath);
        if (sourceDirectory == null || destinationDirectory == null ||
            !string.Equals(sourceDirectory, destinationDirectory, StringComparison.Ordinal))
        {
            // Different case-only directory paths are ambiguous without platform-specific file IDs.
            // Fail closed instead of allowing a backup to overwrite its source.
            return true;
        }

        string sourceName = Path.GetFileName(sourcePath);
        string destinationName = Path.GetFileName(destinationPath);
        bool exactSource = false;
        bool exactDestination = false;
        foreach (string entry in Directory.EnumerateFiles(GetSQLiteFileSystemPath(sourceDirectory)))
        {
            string name = Path.GetFileName(entry);
            exactSource |= string.Equals(name, sourceName, StringComparison.Ordinal);
            exactDestination |= string.Equals(name, destinationName, StringComparison.Ordinal);
            if (exactSource && exactDestination) return false;
        }

        return true;
    }

    /// <summary>
    /// Starts the read transaction a snapshot backup holds on its source and returns the object that ends it. SQLite's
    /// backup step opens and closes its own read transaction only when the source has none, so while this one is open
    /// every step reads the same snapshot and changes by other connections neither appear in the copy nor restart it.
    /// </summary>
    /// <remarks>
    /// The first read retries <c>SQLITE_BUSY</c> (a rollback-journal writer, WAL recovery) under the same
    /// <see cref="SqliteBackupOptions.BusyRetryDelay"/>, <see cref="SqliteBackupOptions.BusyRetryTimeout"/> and
    /// cancellation as the backup steps, and other failures are reported like a failed step.
    /// </remarks>
    private static SourceSnapshot BeginSourceSnapshot(
        SqliteConnection source,
        SqliteBackupOptions options,
        ref TimeSpan busy,
        CancellationToken cancellationToken)
    {
        sqlite3 handle = source.Handle!;
        ThrowIfSnapshotFailed(handle, raw.sqlite3_exec(handle, "BEGIN DEFERRED;"), cancellationToken);
        var snapshot = new SourceSnapshot(handle);
        try
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var attempt = Stopwatch.StartNew();
                int resultCode = ReadSchemaOnce(handle);
                if (resultCode == raw.SQLITE_OK)
                {
                    return snapshot;
                }

                if (!IsBusyResult(resultCode))
                {
                    ThrowIfSnapshotFailed(handle, resultCode, cancellationToken);
                }

                busy += attempt.Elapsed;
                ThrowIfBusyRetryTimeoutExceeded(busy, options.BusyRetryTimeout);
                var delay = Stopwatch.StartNew();
                WaitWithCancellation(options.BusyRetryDelay, cancellationToken);
                busy += delay.Elapsed;
                ThrowIfBusyRetryTimeoutExceeded(busy, options.BusyRetryTimeout);
            }
        }
        catch
        {
            snapshot.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Reads the schema table once inside the open transaction, which starts its read (preparing reads the schema
    /// too, so both steps can meet a lock). Returns <c>SQLITE_OK</c> or the failing result code.
    /// </summary>
    private static int ReadSchemaOnce(sqlite3 handle)
    {
        int resultCode = raw.sqlite3_prepare_v2(handle, "SELECT COUNT(*) FROM sqlite_master;", out sqlite3_stmt statement);
        using (statement)
        {
            if (resultCode != raw.SQLITE_OK)
            {
                return resultCode;
            }

            // Finalizing the statement keeps the read transaction, and with it the snapshot, open.
            resultCode = raw.sqlite3_step(statement);
            return resultCode == raw.SQLITE_ROW || resultCode == raw.SQLITE_DONE ? raw.SQLITE_OK : resultCode;
        }
    }

    /// <summary>Whether a result code is <c>SQLITE_BUSY</c> or <c>SQLITE_LOCKED</c>, extended codes included.</summary>
    private static bool IsBusyResult(int resultCode)
        => (resultCode & 0xFF) == raw.SQLITE_BUSY || (resultCode & 0xFF) == raw.SQLITE_LOCKED;

    private static void ThrowIfSnapshotFailed(sqlite3 handle, int resultCode, CancellationToken cancellationToken)
    {
        if (resultCode == raw.SQLITE_OK)
        {
            return;
        }

        if ((resultCode & 0xFF) == raw.SQLITE_INTERRUPT && cancellationToken.IsCancellationRequested)
        {
            throw new OperationCanceledException(cancellationToken);
        }

        string message = raw.sqlite3_errmsg(handle).utf8_to_string();
        throw CreateBackupProviderException(
            $"Failed to start the SQLite backup snapshot (result code {resultCode}).",
            new InvalidOperationException(message),
            resultCode);
    }

    /// <summary>Ends the snapshot read transaction of <see cref="BeginSourceSnapshot"/>.</summary>
    private sealed class SourceSnapshot : IDisposable
    {
        private sqlite3? _handle;

        internal SourceSnapshot(sqlite3 handle) => _handle = handle;

        public void Dispose()
        {
            sqlite3? handle = Interlocked.Exchange(ref _handle, null);
            if (handle != null && raw.sqlite3_get_autocommit(handle) == 0)
            {
                // A read transaction has nothing to undo; closing the connection would end it as well.
                raw.sqlite3_exec(handle, "ROLLBACK;");
            }
        }
    }

    private static DbaQueryExecutionException CreateBackupProviderException(
        string message,
        Exception exception,
        int providerErrorCode)
        => new(
            message,
            "SQLite online backup",
            exception,
            providerErrorCode,
            providerSqlState: null,
            providerErrorKind: DbaProviderErrorKind.Unknown);

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
    }

    internal static SqliteBackupOptions SnapshotBackupOptions(SqliteBackupOptions? options)
    {
        SqliteBackupOptions source = options ?? new SqliteBackupOptions();
        return new SqliteBackupOptions
        {
            PagesPerStep = source.PagesPerStep,
            StepDelay = source.StepDelay,
            BusyRetryDelay = source.BusyRetryDelay,
            BusyRetryTimeout = source.BusyRetryTimeout,
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
