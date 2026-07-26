using System;

namespace DBAClientX.DataMovement;

/// <summary>
/// Controls reusable table-copy execution behavior.
/// </summary>
public sealed class DbaTableCopyOptions
{
    /// <summary>Default number of rows read from the source per page.</summary>
    public const int DefaultPageSize = 10_000;

    /// <summary>Number of rows requested from the source per page.</summary>
    public int PageSize { get; init; } = DefaultPageSize;

    /// <summary>Optional destination bulk-write batch size.</summary>
    public int? BatchSize { get; init; }

    /// <summary>Optional destination bulk-write timeout in seconds.</summary>
    public int? BulkCopyTimeout { get; init; }

    /// <summary>When true, clears all destination tables in reverse definition order before copying rows.</summary>
    public bool ClearDestination { get; init; }

    /// <summary>When true, compares source and destination row counts after the copy when both adapters can provide counts.</summary>
    public bool VerifyRowCounts { get; init; } = true;

    /// <summary>
    /// Optional non-zero W3C trace identifier used to correlate this copy with caller-owned workflows.
    /// When omitted, the active activity trace identifier or a new identifier is used.
    /// </summary>
    public string? OperationId { get; init; }

    /// <summary>Optional callback that receives copy progress snapshots.</summary>
    public Action<DbaTableCopyProgress>? Progress { get; init; }
}
