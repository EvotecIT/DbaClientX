using System;

namespace DBAClientX.SqlServerManagement;

/// <summary>
/// SQL Server Agent schedule metadata from msdb.
/// </summary>
public sealed class SqlServerAgentScheduleInfo
{
    /// <summary>Optional SQL Server Agent job identifier when the schedule is attached to a job.</summary>
    public Guid? JobId { get; set; }

    /// <summary>Optional SQL Server Agent job name when the schedule is attached to a job.</summary>
    public string? JobName { get; set; }

    /// <summary>Schedule identifier.</summary>
    public int ScheduleId { get; set; }

    /// <summary>Schedule name.</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>True when the schedule is enabled.</summary>
    public bool Enabled { get; set; }

    /// <summary>SQL Server Agent frequency type code.</summary>
    public int FrequencyType { get; set; }

    /// <summary>SQL Server Agent frequency interval code.</summary>
    public int FrequencyInterval { get; set; }

    /// <summary>SQL Server Agent relative interval code for monthly-relative schedules.</summary>
    public int FrequencyRelativeInterval { get; set; }

    /// <summary>SQL Server Agent subday frequency type code.</summary>
    public int FrequencySubdayType { get; set; }

    /// <summary>SQL Server Agent subday frequency interval.</summary>
    public int FrequencySubdayInterval { get; set; }

    /// <summary>SQL Server Agent recurrence factor such as every N weeks or months.</summary>
    public int FrequencyRecurrenceFactor { get; set; }

    /// <summary>Schedule active start date.</summary>
    public DateTime? ActiveStartDate { get; set; }

    /// <summary>Schedule active end date.</summary>
    public DateTime? ActiveEndDate { get; set; }

    /// <summary>Schedule active start time.</summary>
    public TimeSpan? ActiveStartTime { get; set; }

    /// <summary>Schedule active end time.</summary>
    public TimeSpan? ActiveEndTime { get; set; }
}
