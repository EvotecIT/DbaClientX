using System;

namespace DBAClientX;

/// <summary>
/// Describes lightweight SQLite database health and file-state diagnostics.
/// </summary>
public sealed class SqliteDatabaseDiagnostics
{
    /// <summary>
    /// Gets the database path supplied by the caller.
    /// </summary>
    public string Database { get; set; } = string.Empty;

    /// <summary>
    /// Gets the normalized database path when the database is file-backed.
    /// </summary>
    public string FullPath { get; set; } = string.Empty;

    /// <summary>
    /// Gets a value indicating whether the database file exists.
    /// </summary>
    public bool Exists { get; set; }

    /// <summary>
    /// Gets a value indicating whether a read-only connection was opened successfully.
    /// </summary>
    public bool CanConnect { get; set; }

    /// <summary>
    /// Gets a connection or diagnostic error message when one was encountered.
    /// </summary>
    public string? ErrorMessage { get; set; }

    /// <summary>
    /// Gets the SQLite engine version.
    /// </summary>
    public string? SQLiteVersion { get; set; }

    /// <summary>
    /// Gets the result of <c>PRAGMA integrity_check</c>.
    /// </summary>
    public string? IntegrityCheck { get; set; }

    /// <summary>
    /// Gets the result of <c>PRAGMA quick_check</c>.
    /// </summary>
    public string? QuickCheck { get; set; }

    /// <summary>
    /// Gets the current journal mode.
    /// </summary>
    public string? JournalMode { get; set; }

    /// <summary>
    /// Gets the configured synchronous mode.
    /// </summary>
    public string? SynchronousMode { get; set; }

    /// <summary>
    /// Gets the current locking mode.
    /// </summary>
    public string? LockingMode { get; set; }

    /// <summary>
    /// Gets the configured auto-vacuum mode.
    /// </summary>
    public string? AutoVacuumMode { get; set; }

    /// <summary>
    /// Gets the number of database pages.
    /// </summary>
    public long? PageCount { get; set; }

    /// <summary>
    /// Gets the database page size in bytes.
    /// </summary>
    public long? PageSizeBytes { get; set; }

    /// <summary>
    /// Gets the number of pages currently on the freelist.
    /// </summary>
    public long? FreelistCount { get; set; }

    /// <summary>
    /// Gets the schema version.
    /// </summary>
    public long? SchemaVersion { get; set; }

    /// <summary>
    /// Gets the user version.
    /// </summary>
    public long? UserVersion { get; set; }

    /// <summary>
    /// Gets the application id.
    /// </summary>
    public long? ApplicationId { get; set; }

    /// <summary>
    /// Gets the WAL autocheckpoint page threshold.
    /// </summary>
    public long? WalAutoCheckpointPages { get; set; }

    /// <summary>
    /// Gets the main database file size in bytes.
    /// </summary>
    public long DatabaseFileSizeBytes { get; set; }

    /// <summary>
    /// Gets the WAL file size in bytes.
    /// </summary>
    public long WalFileSizeBytes { get; set; }

    /// <summary>
    /// Gets the shared-memory file size in bytes.
    /// </summary>
    public long SharedMemoryFileSizeBytes { get; set; }

    /// <summary>
    /// Gets the approximate logical database size from page count and page size.
    /// </summary>
    public long? LogicalDatabaseSizeBytes =>
        PageCount.HasValue && PageSizeBytes.HasValue
            ? PageCount.Value * PageSizeBytes.Value
            : null;

    /// <summary>
    /// Gets the total size of the database, WAL, and shared-memory files.
    /// </summary>
    public long TotalFileSizeBytes => DatabaseFileSizeBytes + WalFileSizeBytes + SharedMemoryFileSizeBytes;

    /// <summary>
    /// Gets the database file last-write time in UTC.
    /// </summary>
    public DateTimeOffset? LastWriteTimeUtc { get; set; }

    /// <summary>
    /// Gets a value indicating whether both integrity checks report <c>ok</c>.
    /// </summary>
    public bool IsHealthy =>
        CanConnect &&
        string.Equals(IntegrityCheck, "ok", StringComparison.OrdinalIgnoreCase) &&
        string.Equals(QuickCheck, "ok", StringComparison.OrdinalIgnoreCase);
}
