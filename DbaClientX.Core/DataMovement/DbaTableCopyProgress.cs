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
    VerifyDestination
}
