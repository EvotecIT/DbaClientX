using Microsoft.Data.Sqlite;
using SQLitePCL;
using System.Diagnostics;

namespace DBAClientX;

public partial class SQLite
{
    /// <summary>
    /// Copies an SQLite database on a dedicated thread using SQLite's online backup API with the selected method; by
    /// default a WAL database is copied as a held snapshot, which completes while other connections write, and any
    /// other database step-wise.
    /// </summary>
    /// <param name="sourceDatabase">Source SQLite database path.</param>
    /// <param name="destinationDatabase">Destination SQLite database path.</param>
    /// <param name="method">How the source is read: <see cref="SqliteBackupMethod.Auto"/> (the default) picks
    /// <see cref="SqliteBackupMethod.Snapshot"/> when SQLite reports WAL mode and
    /// <see cref="SqliteBackupMethod.Incremental"/> otherwise.</param>
    /// <param name="options">Backup behavior options.</param>
    /// <param name="progress">Optional page-based progress observer.</param>
    /// <param name="cancellationToken">Token used to stop between backup steps and interrupt native work.</param>
    /// <returns>A task containing the completed backup details, including the method that was used.</returns>
    /// <remarks>
    /// The two methods behave as <see cref="BackupDatabaseSnapshotAsync"/> and
    /// <see cref="BackupDatabaseIncrementalAsync"/> describe. <see cref="SqliteBackupMethod.Auto"/> reads
    /// <c>PRAGMA journal_mode</c> through the source connection under the same busy deadline and cancellation as the copy,
    /// so a rollback-journal database keeps the step-wise
    /// copy that never blocks its writers for longer than one step; select <see cref="SqliteBackupMethod.Snapshot"/>
    /// explicitly to copy a rollback-journal database whose writers may wait for the whole copy. The source file is
    /// accessed only through SQLite so that other connections' operating-system locks remain intact.
    /// <para>Invalid arguments, an active client transaction and an existing destination without
    /// <see cref="SqliteBackupOptions.OverwriteDestination"/> throw before the copy starts (the last as
    /// <see cref="IOException"/>). Cancellation surfaces as <see cref="OperationCanceledException"/>, and any other
    /// failure (a provider, file-system or busy-deadline error) as a <see cref="DbaQueryExecutionException"/> whose message
    /// does not contain the paths; <see cref="BackupDatabaseSnapshotAsync"/> and
    /// <see cref="BackupDatabaseIncrementalAsync"/> throw those failures unwrapped.</para>
    /// </remarks>
    public virtual Task<SqliteBackupResult> BackupDatabaseAsync(
        string sourceDatabase,
        string destinationDatabase,
        SqliteBackupMethod method = SqliteBackupMethod.Auto,
        SqliteBackupOptions? options = null,
        IProgress<SqliteBackupProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        ValidateDatabasePath(sourceDatabase);
        ValidateDatabasePath(destinationDatabase);
        string sourcePath = Path.GetFullPath(sourceDatabase);
        string destinationPath = Path.GetFullPath(destinationDatabase);
        if (AreSameBackupPath(sourcePath, destinationPath))
        {
            throw new ArgumentException("Source and destination database paths must be different.", nameof(destinationDatabase));
        }
        if (options?.OverwriteDestination != true && File.Exists(sourcePath) && File.Exists(destinationPath))
        {
            throw new IOException($"SQLite backup destination already exists: {destinationPath}");
        }

        return ReportBackupFailuresAsync(
            StartBackup(sourceDatabase, destinationDatabase, method, options, progress, cancellationToken));
    }

    /// <summary>
    /// Reports the operational failures of a started backup as a <see cref="DbaQueryExecutionException"/> without the
    /// paths in its message, leaving cancellation and argument errors as they are.
    /// </summary>
    private async Task<SqliteBackupResult> ReportBackupFailuresAsync(Task<SqliteBackupResult> backup)
    {
        try
        {
            return await backup.ConfigureAwait(false);
        }
        catch (DbaQueryExecutionException)
        {
            throw;
        }
        catch (Exception exception) when (
            exception is SqliteException or IOException or UnauthorizedAccessException or
            InvalidOperationException or NotSupportedException or TimeoutException)
        {
            throw CreateQueryExecutionException(
                "Failed to back up SQLite database.",
                "SQLite online backup",
                exception);
        }
    }

    private static void ValidateBackupMethod(SqliteBackupMethod method)
    {
        if (method is not (SqliteBackupMethod.Auto or SqliteBackupMethod.Snapshot or SqliteBackupMethod.Incremental))
        {
            throw new ArgumentOutOfRangeException(nameof(method), method, "Unknown SQLite backup method.");
        }
    }

    /// <summary>
    /// Turns <see cref="SqliteBackupMethod.Auto"/> into the method used for <paramref name="source"/>; the other
    /// methods are returned unchanged.
    /// </summary>
    private static SqliteBackupMethod ResolveBackupMethod(SqliteBackupMethod method, SqliteConnection source,
        SqliteBackupOptions options, ref TimeSpan busy, CancellationToken cancellationToken)
    {
        if (method != SqliteBackupMethod.Auto)
        {
            return method;
        }

        sqlite3 handle = source.Handle!;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var attempt = Stopwatch.StartNew();
            int resultCode = raw.sqlite3_prepare_v2(handle, "PRAGMA journal_mode;", out sqlite3_stmt statement);
            using (statement)
            {
                if (resultCode == raw.SQLITE_OK) resultCode = raw.sqlite3_step(statement);
                if (resultCode == raw.SQLITE_ROW)
                {
                    string journalMode = raw.sqlite3_column_text(statement, 0).utf8_to_string();
                    return string.Equals(journalMode, "wal", StringComparison.OrdinalIgnoreCase)
                        ? SqliteBackupMethod.Snapshot : SqliteBackupMethod.Incremental;
                }
            }
            if (!IsBusyResult(resultCode))
            {
                if ((resultCode & 0xFF) == raw.SQLITE_INTERRUPT && cancellationToken.IsCancellationRequested)
                    throw new OperationCanceledException(cancellationToken);
                throw CreateBackupProviderException("Failed to determine the SQLite backup journal mode.",
                    new InvalidOperationException(raw.sqlite3_errmsg(handle).utf8_to_string()), resultCode);
            }
            busy += attempt.Elapsed;
            ThrowIfBusyRetryTimeoutExceeded(busy, options.BusyRetryTimeout);
            var delay = Stopwatch.StartNew();
            WaitWithCancellation(options.BusyRetryDelay, cancellationToken);
            busy += delay.Elapsed;
            ThrowIfBusyRetryTimeoutExceeded(busy, options.BusyRetryTimeout);
        }
    }
}
