using System;

namespace DBAClientX.SqlServerMonitoring;

/// <summary>
/// Database state and accessibility information from <c>sys.databases</c>.
/// </summary>
public sealed class SqlServerDatabaseState
{
    /// <summary>Database name.</summary>
    public string DatabaseName { get; set; } = string.Empty;

    /// <summary>Database state such as ONLINE, RESTORING, or SUSPECT.</summary>
    public string Status { get; set; } = string.Empty;

    /// <summary>User access mode such as MULTI_USER or SINGLE_USER.</summary>
    public string Access { get; set; } = string.Empty;

    /// <summary>Read/write state such as READ_WRITE or READ_ONLY.</summary>
    public string ReadWrite { get; set; } = string.Empty;

    /// <summary>Database recovery model.</summary>
    public string RecoveryModel { get; set; } = string.Empty;

    /// <summary>True when the database is a system database.</summary>
    public bool IsSystemDatabase { get; set; }

    /// <summary>UTC or server-local creation timestamp reported by SQL Server.</summary>
    public DateTime? CreateDate { get; set; }

    /// <summary>True when the database state is generally healthy for normal monitoring.</summary>
    public bool IsHealthy => string.Equals(Status, "ONLINE", StringComparison.OrdinalIgnoreCase) &&
                             string.Equals(Access, "MULTI_USER", StringComparison.OrdinalIgnoreCase);
}
