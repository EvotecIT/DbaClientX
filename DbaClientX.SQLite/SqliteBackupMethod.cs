namespace DBAClientX;

/// <summary>
/// Selects how an SQLite online backup reads its source.
/// </summary>
public enum SqliteBackupMethod
{
    /// <summary>
    /// Uses <see cref="Snapshot"/> for a database in WAL mode and <see cref="Incremental"/> otherwise, as read from the
    /// source connection's <c>PRAGMA journal_mode</c> when the backup starts.
    /// </summary>
    Auto,

    /// <summary>
    /// Holds one read transaction on the source across every backup step, so the copy completes while other
    /// connections write and contains the database as it was when the backup started. In rollback-journal mode the
    /// held read blocks writers until the backup ends.
    /// </summary>
    Snapshot,

    /// <summary>
    /// Releases the source between backup steps. Writers are never blocked for longer than one step, but SQLite
    /// restarts the copy whenever another connection changes the source, so a database written continuously is never
    /// copied. Suits rollback-journal databases and databases that are not being written.
    /// </summary>
    Incremental
}
