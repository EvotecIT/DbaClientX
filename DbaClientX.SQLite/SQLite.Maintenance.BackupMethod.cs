using Microsoft.Data.Sqlite;

namespace DBAClientX;

public partial class SQLite
{
    private const int DatabaseHeaderLength = 20;
    private const byte WalFileFormatVersion = 2;
    private static readonly byte[] DatabaseHeaderMagic = System.Text.Encoding.ASCII.GetBytes("SQLite format 3\0");

    /// <summary>
    /// Copies an SQLite database on a dedicated thread using SQLite's online backup API with the selected method; by
    /// default a WAL database is copied as a held snapshot, which completes while other connections write, and any
    /// other database step-wise.
    /// </summary>
    /// <param name="sourceDatabase">Source SQLite database path.</param>
    /// <param name="destinationDatabase">Destination SQLite database path.</param>
    /// <param name="method">How the source is read: <see cref="SqliteBackupMethod.Auto"/> (the default) picks
    /// <see cref="SqliteBackupMethod.Snapshot"/> when the source file header says WAL mode and
    /// <see cref="SqliteBackupMethod.Incremental"/> otherwise.</param>
    /// <param name="options">Backup behavior options.</param>
    /// <param name="progress">Optional page-based progress observer.</param>
    /// <param name="cancellationToken">Token used to stop between backup steps and interrupt native work.</param>
    /// <returns>A task containing the completed backup details, including the method that was used.</returns>
    /// <remarks>
    /// The two methods behave as <see cref="BackupDatabaseSnapshotAsync"/> and
    /// <see cref="BackupDatabaseIncrementalAsync"/> describe. <see cref="SqliteBackupMethod.Auto"/> reads the file
    /// format versions of the source header without taking a lock, so a rollback-journal database keeps the step-wise
    /// copy that never blocks its writers for longer than one step; select <see cref="SqliteBackupMethod.Snapshot"/>
    /// explicitly to copy a rollback-journal database whose writers may wait for the whole copy. A file whose header
    /// is not a plain SQLite header (an encrypted database, for example) is treated as not WAL.
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
    /// Turns <see cref="SqliteBackupMethod.Auto"/> into the method used for <paramref name="sourcePath"/>; the other
    /// methods are returned unchanged.
    /// </summary>
    internal static SqliteBackupMethod ResolveBackupMethod(SqliteBackupMethod method, string sourcePath)
    {
        if (method != SqliteBackupMethod.Auto)
        {
            return method;
        }

        return IsWalDatabaseFile(sourcePath) ? SqliteBackupMethod.Snapshot : SqliteBackupMethod.Incremental;
    }

    /// <summary>
    /// Whether the database file header records WAL mode. Bytes 18 and 19 of the header are the file format write and
    /// read versions, which SQLite sets to 2 when a database is switched to WAL and to 1 when it leaves it. The header
    /// is read with full sharing and no SQLite lock, so a writer holding the database does not delay the decision.
    /// </summary>
    internal static bool IsWalDatabaseFile(string path)
    {
        var header = new byte[DatabaseHeaderLength];
        using (var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete))
        {
            int read = 0;
            while (read < header.Length)
            {
                int count = stream.Read(header, read, header.Length - read);
                if (count == 0)
                {
                    return false;
                }

                read += count;
            }
        }

        for (int index = 0; index < DatabaseHeaderMagic.Length; index++)
        {
            if (header[index] != DatabaseHeaderMagic[index])
            {
                return false;
            }
        }

        return header[18] == WalFileFormatVersion && header[19] == WalFileFormatVersion;
    }
}
