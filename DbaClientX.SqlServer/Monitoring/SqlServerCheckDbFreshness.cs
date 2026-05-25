using System;

namespace DBAClientX.SqlServerMonitoring;

/// <summary>
/// Last known good DBCC CHECKDB information for one database.
/// </summary>
public sealed class SqlServerCheckDbFreshness
{
    /// <summary>Database name.</summary>
    public string DatabaseName { get; set; } = string.Empty;

    /// <summary>Database creation timestamp.</summary>
    public DateTime? DatabaseCreated { get; set; }

    /// <summary>Last known good CHECKDB timestamp when it could be collected.</summary>
    public DateTime? LastGoodCheckDb { get; set; }

    /// <summary>Elapsed time since the last known good CHECKDB.</summary>
    public TimeSpan? SinceLastGoodCheckDb { get; set; }

    /// <summary>Provider-level status computed from configured thresholds.</summary>
    public string Status { get; set; } = "Unknown";

    /// <summary>Error message for this database when CHECKDB metadata could not be collected.</summary>
    public string? ErrorMessage { get; set; }
}
