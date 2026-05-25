using System;

namespace DBAClientX.SqlServerMonitoring;

/// <summary>
/// Backup recency information for one database.
/// </summary>
public sealed class SqlServerBackupFreshness
{
    /// <summary>Database name.</summary>
    public string DatabaseName { get; set; } = string.Empty;

    /// <summary>Database recovery model.</summary>
    public string RecoveryModel { get; set; } = string.Empty;

    /// <summary>Database creation timestamp.</summary>
    public DateTime? DatabaseCreated { get; set; }

    /// <summary>Most recent full backup completion time.</summary>
    public DateTime? LastFullBackup { get; set; }

    /// <summary>Most recent differential backup completion time.</summary>
    public DateTime? LastDifferentialBackup { get; set; }

    /// <summary>Most recent log backup completion time.</summary>
    public DateTime? LastLogBackup { get; set; }

    /// <summary>Elapsed time since the most recent full backup.</summary>
    public TimeSpan? SinceFullBackup { get; set; }

    /// <summary>Elapsed time since the most recent differential backup.</summary>
    public TimeSpan? SinceDifferentialBackup { get; set; }

    /// <summary>Elapsed time since the most recent log backup.</summary>
    public TimeSpan? SinceLogBackup { get; set; }

    /// <summary>True when the database uses full or bulk-logged recovery and should normally have log backups.</summary>
    public bool RequiresLogBackups { get; set; }

    /// <summary>Provider-level status computed from configured thresholds.</summary>
    public string Status { get; set; } = "Unknown";
}
