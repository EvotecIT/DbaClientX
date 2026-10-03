namespace DBAClientX.SqlServerMonitoring;

/// <summary>Query Store configuration reported by the target database; collecting it does not enable or alter Query Store.</summary>
public sealed class SqlServerQueryStoreState
{
    /// <summary>Database whose Query Store configuration was read.</summary>
    public string DatabaseName { get; set; } = string.Empty;

    /// <summary>Configured operating state, as reported by SQL Server.</summary>
    public string DesiredState { get; set; } = string.Empty;

    /// <summary>Actual operating state, which can differ from the configured state.</summary>
    public string ActualState { get; set; } = string.Empty;

    /// <summary>Query capture policy, as reported by SQL Server.</summary>
    public string CaptureMode { get; set; } = string.Empty;

    /// <summary>Current storage in megabytes.</summary>
    public long CurrentStorageSizeMb { get; set; }

    /// <summary>Configured storage limit in megabytes.</summary>
    public long MaximumStorageSizeMb { get; set; }

    /// <summary>SQL Server's read-only reason bitmask, retained without discarding unknown or combined flags.</summary>
    public long ReadOnlyReason { get; set; }
}
