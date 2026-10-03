namespace DBAClientX.DataMovement;

/// <summary>
/// Reports progress for one table-data copy operation.
/// </summary>
public sealed record DbaTableCopyProgress(
    string TableName,
    long RowsCopied,
    long? SourceRows,
    int PageRows)
{
    /// <summary>Operation phase; row counters describe the work in this phase.</summary>
    public DbaTableCopyPhase Phase { get; init; }

    /// <summary>Elapsed time in the current table/phase invocation, including adapter waits and earlier progress callbacks.</summary>
    public TimeSpan? Elapsed { get; init; }

    /// <summary>Rows processed in this invocation; previously committed resume rows are excluded.</summary>
    public long? RowsProcessedThisPass { get; init; }

    /// <summary>Average rows processed per second in this invocation, or null before measurable progress.</summary>
    public double? RowsPerSecond => Elapsed is { TotalSeconds: > 0 } elapsed && RowsProcessedThisPass is > 0
        ? RowsProcessedThisPass.Value / elapsed.TotalSeconds : null;

    /// <summary>Estimated time for the remaining rows in this phase, or null when row count or rate is unknown. Later phases are excluded.</summary>
    public TimeSpan? EstimatedRemaining
    {
        get
        {
            if (!SourceRows.HasValue || SourceRows.Value < 0) return null;
            if (RowsCopied >= SourceRows.Value) return TimeSpan.Zero;
            if (RowsPerSecond is not double rate || double.IsNaN(rate) || double.IsInfinity(rate) || rate <= 0) return null;
            double seconds = (SourceRows.Value - (double)RowsCopied) / rate;
            return seconds < TimeSpan.MaxValue.TotalSeconds ? TimeSpan.FromSeconds(seconds) : null;
        }
    }
    /// <summary>Copy percentage when source row count is known.</summary>
    public double? PercentComplete
        => SourceRows is > 0
            ? RowsCopied >= SourceRows.Value ? 100d : RowsCopied * 100d / SourceRows.Value
            : null;
}

/// <summary>Identifies the work reported by a table-copy progress event.</summary>
public enum DbaTableCopyPhase
{
    /// <summary>Writing source rows to the destination.</summary>
    Copy,
    /// <summary>Validating and checksumming the source before any destination is changed.</summary>
    ValidateSource,
    /// <summary>Checking destination contents against committed or complete source data.</summary>
    VerifyDestination,
    /// <summary>Checking source pages and destination schema compatibility before clearing tables.</summary>
    PreflightSource,
    /// <summary>Checking or preparing destination tables and checkpoints before copying.</summary>
    PrepareDestination
}
