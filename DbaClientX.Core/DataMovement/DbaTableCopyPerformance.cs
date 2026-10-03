namespace DBAClientX.DataMovement;

/// <summary>Optional measurements of the pages processed by a completed copy run.</summary>
public sealed record DbaTableCopyPerformance
{
    /// <summary>Completed phase invocations in execution order, including repeated verification passes.</summary>
    public IReadOnlyList<DbaTableCopyPhaseStatistics> Phases { get; init; } = Array.Empty<DbaTableCopyPhaseStatistics>();

    /// <summary>Source page requests completed during this run, including validation and preflight reads.</summary>
    public long SourcePageCount => Phases.Sum(static phase => phase.SourcePageCount);

    /// <summary>Source rows materialized during this run, including repeated reads.</summary>
    public long SourceRowsRead => Phases.Sum(static phase => phase.SourceRowsRead);

    /// <summary>Estimated managed payload of source pages, including repeated reads; excludes native buffers and network traffic.</summary>
    public long EstimatedSourcePayloadBytes => Phases.Sum(static phase => phase.EstimatedSourcePayloadBytes);

    /// <summary>Destination rows materialized for probes and verification during this run.</summary>
    public long DestinationRowsRead => Phases.Sum(static phase => phase.DestinationRowsRead);

    /// <summary>Rows successfully written during this run; previously committed rows are excluded.</summary>
    public long RowsWritten => Phases.Sum(static phase => phase.RowsWritten);

    /// <summary>Estimated managed payload of successfully written pages; excludes native buffers and network traffic.</summary>
    public long EstimatedWrittenPayloadBytes => Phases.Sum(static phase => phase.EstimatedWrittenPayloadBytes);
}

/// <summary>Measurements for one phase invocation. Batch preflight can cover several tables.</summary>
public sealed record DbaTableCopyPhaseStatistics
{
    /// <summary>The work performed by this invocation.</summary>
    public DbaTableCopyPhase Phase { get; init; }

    /// <summary>Sanitized logical table name, or null for an invocation covering multiple tables.</summary>
    public string? TableName { get; init; }

    /// <summary>Elapsed time excluding nested engine phases. Includes adapter waits, transformation and caller progress callbacks.</summary>
    public TimeSpan Duration { get; init; }

    /// <summary>Completed source page requests.</summary>
    public long SourcePageCount { get; init; }

    /// <summary>Source page requests that start with no continuation token. A later phase can reuse an earlier first page.</summary>
    public long SourcePageStreamCount { get; init; }

    /// <summary>Source rows materialized, including repeated reads.</summary>
    public long SourceRowsRead { get; init; }

    /// <summary>Estimated managed source page payload using the same estimator as the page byte budget.</summary>
    public long EstimatedSourcePayloadBytes { get; init; }

    /// <summary>Completed destination page requests for probes or verification.</summary>
    public long DestinationPageCount { get; init; }

    /// <summary>Destination rows materialized for probes or verification.</summary>
    public long DestinationRowsRead { get; init; }

    /// <summary>Estimated managed destination page payload.</summary>
    public long EstimatedDestinationPayloadBytes { get; init; }

    /// <summary>Pages successfully committed or written; excludes checkpoint completion markers.</summary>
    public long WrittenPageCount { get; init; }

    /// <summary>Rows successfully committed or written during this run.</summary>
    public long RowsWritten { get; init; }

    /// <summary>Estimated managed payload of successfully committed or written pages.</summary>
    public long EstimatedWrittenPayloadBytes { get; init; }
}
