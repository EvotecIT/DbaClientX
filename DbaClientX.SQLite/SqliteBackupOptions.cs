namespace DBAClientX;

/// <summary>
/// Controls an incremental SQLite online backup operation.
/// </summary>
public sealed class SqliteBackupOptions
{
    /// <summary>
    /// Gets or sets the number of database pages copied by each online-backup step.
    /// Smaller values release the source read lock more frequently.
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
    /// Gets or sets an optional SQLite busy timeout applied to the source and destination connections.
    /// </summary>
    public int? BusyTimeoutMs { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether an existing destination file may be replaced.
    /// </summary>
    public bool OverwriteDestination { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether a destination created by a failed or canceled backup is deleted.
    /// </summary>
    public bool DeleteDestinationOnFailure { get; set; } = true;
}
