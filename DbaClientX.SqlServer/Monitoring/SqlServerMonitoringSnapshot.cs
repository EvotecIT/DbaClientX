using System;
using System.Collections.Generic;

namespace DBAClientX.SqlServerMonitoring;

/// <summary>
/// Aggregated SQL Server monitoring snapshot collected for one target.
/// </summary>
public sealed class SqlServerMonitoringSnapshot
{
    /// <summary>Requested SQL Server target.</summary>
    public string Target { get; set; } = string.Empty;

    /// <summary>UTC timestamp when collection finished.</summary>
    public DateTimeOffset CompletedUtc { get; set; } = DateTimeOffset.UtcNow;

    /// <summary>Scopes requested for this snapshot.</summary>
    public SqlServerMonitoringScope RequestedScope { get; set; }

    /// <summary>Connection diagnostics. This is always populated when connectivity was requested.</summary>
    public SqlServerConnectionDiagnostics? Connectivity { get; set; }

    /// <summary>Database state rows.</summary>
    public List<SqlServerDatabaseState> Databases { get; set; } = new();

    /// <summary>Backup freshness rows.</summary>
    public List<SqlServerBackupFreshness> Backups { get; set; } = new();

    /// <summary>CHECKDB freshness rows.</summary>
    public List<SqlServerCheckDbFreshness> CheckDb { get; set; } = new();

    /// <summary>SQL Agent job health rows.</summary>
    public List<SqlServerAgentJobHealth> AgentJobs { get; set; } = new();

    /// <summary>SQL wait statistic rows.</summary>
    public List<SqlServerWaitStatistic> WaitStatistics { get; set; } = new();

    /// <summary>Availability Group health rows.</summary>
    public List<SqlServerAvailabilityGroupHealth> AvailabilityGroups { get; set; } = new();

    /// <summary>Query Store configuration for the target database; null when not requested or unavailable (see <see cref="Errors"/>).</summary>
    public SqlServerQueryStoreState? QueryStore { get; set; }

    /// <summary>Bounded index usage and statistics for the target database; null when not requested or unavailable.</summary>
    public SqlServerIndexUsageSnapshot? IndexUsage { get; set; }

    /// <summary>Collector errors for optional sections that could not be read.</summary>
    public List<string> Errors { get; set; } = new();
}
