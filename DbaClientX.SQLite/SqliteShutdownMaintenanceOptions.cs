namespace DBAClientX;

/// <summary>
/// Controls graceful shutdown maintenance executed against a SQLite database.
/// </summary>
public sealed class SqliteShutdownMaintenanceOptions
{
    /// <summary>
    /// Gets or sets the checkpoint mode applied before the connection is closed.
    /// </summary>
    public SqliteCheckpointMode CheckpointMode { get; set; } = SqliteCheckpointMode.Truncate;

    /// <summary>
    /// Gets or sets the optional busy timeout in milliseconds used by the maintenance connection.
    /// </summary>
    /// <remarks>
    /// Set to <see langword="null"/> to use the client instance <see cref="SQLite.BusyTimeoutMs"/>.
    /// </remarks>
    public int? BusyTimeoutMs { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether <c>PRAGMA optimize</c> should be executed after checkpointing.
    /// </summary>
    public bool OptimizeAfterCheckpoint { get; set; } = true;
}

/// <summary>
/// SQLite checkpoint modes supported by <c>PRAGMA wal_checkpoint</c>.
/// </summary>
public enum SqliteCheckpointMode
{
    /// <summary>
    /// Passive checkpointing that does not block active readers or writers.
    /// </summary>
    Passive = 0,

    /// <summary>
    /// Full checkpointing that waits for active writers to finish.
    /// </summary>
    Full = 1,

    /// <summary>
    /// Restart checkpointing that resets the WAL once checkpointing finishes.
    /// </summary>
    Restart = 2,

    /// <summary>
    /// Truncate checkpointing that truncates the WAL file after checkpointing completes.
    /// </summary>
    Truncate = 3
}
