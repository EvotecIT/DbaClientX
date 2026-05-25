namespace DBAClientX.SqlServerMonitoring;

/// <summary>
/// SQL Server wait statistic row collected from <c>sys.dm_os_wait_stats</c>.
/// </summary>
public sealed class SqlServerWaitStatistic
{
    /// <summary>Wait type name.</summary>
    public string WaitType { get; set; } = string.Empty;

    /// <summary>Total wait time in seconds.</summary>
    public decimal WaitSeconds { get; set; }

    /// <summary>Resource wait time in seconds.</summary>
    public decimal ResourceSeconds { get; set; }

    /// <summary>Signal wait time in seconds.</summary>
    public decimal SignalSeconds { get; set; }

    /// <summary>Total wait count.</summary>
    public long WaitCount { get; set; }

    /// <summary>Percentage of total non-ignored wait time represented by this wait.</summary>
    public decimal Percentage { get; set; }

    /// <summary>Average wait time in seconds.</summary>
    public decimal AverageWaitSeconds { get; set; }
}
