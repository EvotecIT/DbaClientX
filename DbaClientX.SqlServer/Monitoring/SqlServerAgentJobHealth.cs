using System;

namespace DBAClientX.SqlServerMonitoring;

/// <summary>
/// SQL Server Agent job health and recent execution state.
/// </summary>
public sealed class SqlServerAgentJobHealth
{
    /// <summary>SQL Agent job identifier.</summary>
    public Guid JobId { get; set; }

    /// <summary>SQL Agent job name.</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>Job category name.</summary>
    public string? Category { get; set; }

    /// <summary>Job owner login name.</summary>
    public string? OwnerLoginName { get; set; }

    /// <summary>True when the job is enabled.</summary>
    public bool Enabled { get; set; }

    /// <summary>True when SQL Agent currently reports the job as running.</summary>
    public bool IsRunning { get; set; }

    /// <summary>Timestamp when the current execution started, if running.</summary>
    public DateTime? CurrentStartDate { get; set; }

    /// <summary>Most recent job run timestamp.</summary>
    public DateTime? LastRunDate { get; set; }

    /// <summary>Most recent job outcome such as Succeeded or Failed.</summary>
    public string? LastRunOutcome { get; set; }

    /// <summary>Last run duration in SQL Agent history when available.</summary>
    public TimeSpan? LastRunDuration { get; set; }
}
