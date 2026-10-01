namespace DBAClientX;

/// <summary>
/// Controls an SQLite online backup (<c>SQLite.BackupDatabaseIncrementalAsync</c> or
/// <c>SQLite.BackupDatabaseSnapshotAsync</c>).
/// </summary>
public sealed class SqliteBackupOptions
{
    /// <summary>
    /// Gets or sets the number of database pages copied by each online-backup step; progress is reported and
    /// cancellation checked after each step. An incremental backup releases the source read lock between steps, so
    /// smaller values let writers in more often; a snapshot backup holds its snapshot across steps. Values above 4096
    /// are rejected so cancellation remains responsive while native backup work is in progress.
    /// </summary>
    public int PagesPerStep { get; set; } = 256;

    /// <summary>
    /// Gets or sets an optional delay between successful backup steps.
    /// </summary>
    public TimeSpan StepDelay { get; set; } = TimeSpan.Zero;

    /// <summary>
    /// Gets or sets the delay before retrying a busy or locked backup step.
    /// </summary>
    public TimeSpan BusyRetryDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Gets or sets the maximum cumulative elapsed time allowed while the backup remains busy or locked.
    /// Set to <see cref="TimeSpan.Zero"/> to retry without an additional deadline.
    /// </summary>
    public TimeSpan BusyRetryTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets a value indicating whether an existing destination file may be replaced.
    /// </summary>
    public bool OverwriteDestination { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether a destination created by a failed or canceled backup is deleted.
    /// </summary>
    public bool DeleteDestinationOnFailure { get; set; } = true;
}
