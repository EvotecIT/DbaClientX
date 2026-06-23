using System;

namespace DBAClientX.SqlServerManagement;

/// <summary>
/// SQL Server Agent job step definition metadata from msdb.
/// </summary>
public sealed class SqlServerAgentJobStepInfo
{
    /// <summary>SQL Server Agent job identifier.</summary>
    public Guid JobId { get; set; }

    /// <summary>SQL Server Agent job name.</summary>
    public string JobName { get; set; } = string.Empty;

    /// <summary>Step ordinal within the job.</summary>
    public int StepId { get; set; }

    /// <summary>Step name.</summary>
    public string StepName { get; set; } = string.Empty;

    /// <summary>Step subsystem such as TSQL, CmdExec, or PowerShell.</summary>
    public string Subsystem { get; set; } = string.Empty;

    /// <summary>Command text executed by the step.</summary>
    public string? Command { get; set; }

    /// <summary>Database used by the step when applicable.</summary>
    public string? DatabaseName { get; set; }

    /// <summary>Action taken when the step succeeds.</summary>
    public string? OnSuccessAction { get; set; }

    /// <summary>Action taken when the step fails.</summary>
    public string? OnFailAction { get; set; }

    /// <summary>Number of retry attempts configured for the step.</summary>
    public int RetryAttempts { get; set; }

    /// <summary>Retry interval in minutes.</summary>
    public int RetryInterval { get; set; }
}
