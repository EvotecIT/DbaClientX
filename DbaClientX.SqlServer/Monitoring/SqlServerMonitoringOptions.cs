using System;

namespace DBAClientX.SqlServerMonitoring;

/// <summary>
/// Controls how SQL Server monitoring data is collected and evaluated by the provider.
/// </summary>
public sealed class SqlServerMonitoringOptions
{
    /// <summary>Monitoring areas to collect.</summary>
    public SqlServerMonitoringScope Scope { get; set; } = SqlServerMonitoringScope.Baseline;

    /// <summary>Maximum age for full database backups before consumers should consider them overdue.</summary>
    public TimeSpan MaxFullBackupAge { get; set; } = TimeSpan.FromDays(1);

    /// <summary>Maximum age for differential backups before consumers should consider them overdue.</summary>
    public TimeSpan MaxDifferentialBackupAge { get; set; } = TimeSpan.FromDays(1);

    /// <summary>Maximum age for log backups on full or bulk-logged recovery databases.</summary>
    public TimeSpan MaxLogBackupAge { get; set; } = TimeSpan.FromHours(1);

    /// <summary>Maximum age for last known good CHECKDB before consumers should consider it overdue.</summary>
    public TimeSpan MaxCheckDbAge { get; set; } = TimeSpan.FromDays(7);

    /// <summary>When true, system databases are included in database-level collectors.</summary>
    public bool IncludeSystemDatabases { get; set; }

    /// <summary>When true, disabled SQL Agent jobs are included in job health output.</summary>
    public bool IncludeDisabledAgentJobs { get; set; }

    /// <summary>Maximum cumulative wait percentage to include in wait-stat output.</summary>
    public decimal WaitStatisticThresholdPercent { get; set; } = 95m;

    /// <summary>Returns true when the requested scope includes the supplied flag.</summary>
    /// <param name="scope">Scope flag to test.</param>
    /// <returns><see langword="true"/> when <see cref="Scope"/> contains <paramref name="scope"/>.</returns>
    public bool Includes(SqlServerMonitoringScope scope) => (Scope & scope) == scope;
}
