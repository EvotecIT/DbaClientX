namespace DBAClientX.SqlServerMonitoring;

/// <summary>
/// Availability Group replica and database health row.
/// </summary>
public sealed class SqlServerAvailabilityGroupHealth
{
    /// <summary>Availability Group name.</summary>
    public string AvailabilityGroupName { get; set; } = string.Empty;

    /// <summary>Replica server name.</summary>
    public string ReplicaServerName { get; set; } = string.Empty;

    /// <summary>Replica role such as PRIMARY or SECONDARY.</summary>
    public string? Role { get; set; }

    /// <summary>Replica operational state.</summary>
    public string? OperationalState { get; set; }

    /// <summary>Replica connection state.</summary>
    public string? ConnectedState { get; set; }

    /// <summary>Replica synchronization health.</summary>
    public string? SynchronizationHealth { get; set; }

    /// <summary>Database name when the row is database-scoped.</summary>
    public string? DatabaseName { get; set; }

    /// <summary>Database synchronization state.</summary>
    public string? DatabaseSynchronizationState { get; set; }

    /// <summary>Database synchronization health.</summary>
    public string? DatabaseSynchronizationHealth { get; set; }

    /// <summary>True when the database replica is suspended.</summary>
    public bool IsSuspended { get; set; }

    /// <summary>Database replica suspend reason.</summary>
    public string? SuspendReason { get; set; }

    /// <summary>True when the replica/database row looks healthy.</summary>
    public bool IsHealthy =>
        !IsSuspended &&
        HasState(ConnectedState, "CONNECTED") &&
        HasState(SynchronizationHealth, "HEALTHY") &&
        (string.IsNullOrWhiteSpace(DatabaseName) || HasState(DatabaseSynchronizationHealth, "HEALTHY"));

    private static bool HasState(string? actual, string expected)
    {
        return !string.IsNullOrWhiteSpace(actual) &&
               string.Equals(actual, expected, System.StringComparison.OrdinalIgnoreCase);
    }
}
