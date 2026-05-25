using System;

namespace DBAClientX.SqlServerMonitoring;

/// <summary>
/// SQL Server monitoring areas that can be collected for a target instance.
/// </summary>
[Flags]
public enum SqlServerMonitoringScope
{
    /// <summary>No monitoring data beyond connectivity should be collected.</summary>
    None = 0,

    /// <summary>Collect connection, server identity, TCP, encryption, and authentication diagnostics.</summary>
    Connectivity = 1 << 0,

    /// <summary>Collect user database state, access mode, recovery model, and read/write state.</summary>
    DatabaseState = 1 << 1,

    /// <summary>Collect backup freshness from <c>msdb</c> backup history.</summary>
    BackupFreshness = 1 << 2,

    /// <summary>Collect last known good DBCC CHECKDB timestamps where permissions allow it.</summary>
    CheckDbFreshness = 1 << 3,

    /// <summary>Collect SQL Server Agent job state and recent outcome information.</summary>
    AgentJobs = 1 << 4,

    /// <summary>Collect wait statistics for performance diagnostics.</summary>
    WaitStatistics = 1 << 5,

    /// <summary>Collect Availability Group replica and database synchronization health.</summary>
    AvailabilityGroups = 1 << 6,

    /// <summary>Recommended baseline monitoring for continuous health checks.</summary>
    Baseline = Connectivity | DatabaseState | BackupFreshness | CheckDbFreshness | AgentJobs,

    /// <summary>All monitoring areas currently implemented by this provider.</summary>
    All = Baseline | WaitStatistics | AvailabilityGroups
}
