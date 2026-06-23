using System;

namespace DBAClientX.SqlServerManagement;

/// <summary>
/// SQL Server Agent job definition metadata from msdb.
/// </summary>
public sealed class SqlServerAgentJobInfo
{
    /// <summary>SQL Server Agent job identifier.</summary>
    public Guid JobId { get; set; }

    /// <summary>SQL Server Agent job name.</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>Job category name.</summary>
    public string? Category { get; set; }

    /// <summary>Job owner login name.</summary>
    public string? OwnerLoginName { get; set; }

    /// <summary>Job description.</summary>
    public string? Description { get; set; }

    /// <summary>True when the job is enabled.</summary>
    public bool Enabled { get; set; }

    /// <summary>Date the job was created.</summary>
    public DateTime? Created { get; set; }

    /// <summary>Date the job was last modified.</summary>
    public DateTime? Modified { get; set; }
}
