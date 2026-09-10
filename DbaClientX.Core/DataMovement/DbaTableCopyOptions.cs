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

    /// <summary>Optional destination bulk-write timeout in seconds. Specify 0 for no timeout.</summary>
    public int? BulkCopyTimeout { get; init; }

    /// <summary>When true, clears all destination tables in reverse definition order before copying rows.</summary>
    public bool ClearDestination { get; init; }

    /// <summary>When true, compares source and destination row counts after the copy when both adapters can provide counts.</summary>
    public bool VerifyRowCounts { get; init; } = true;

    /// <summary>Verifies every copied column using a provider-neutral SHA-256 multiset checksum, in addition to row counts.</summary>
    public bool VerifyContent { get; init; }

    /// <summary>Rejects a nonempty destination before any table is changed unless ClearDestination or Resume is set.</summary>
    public bool RequireEmptyDestination { get; init; }

    /// <summary>Optional estimated memory limit for each source or verification page; requires bounded provider reads.</summary>
    public long? MaxPageBytes { get; init; }

    /// <summary>Preserves SQL Server identity values from the source. All identity columns must be included when enabled.</summary>
    public bool KeepIdentity { get; init; }

    /// <summary>
    /// Optional stable copy identifier. SQLite and SQL Server destinations commit a durable checkpoint atomically
    /// with each page. Use a new identifier for a new copy and retain it for an explicit resume.
    /// </summary>
    public string? CheckpointId { get; init; }

    /// <summary>Continues an existing CheckpointId after validating the source and committed destination contents.</summary>
    public bool Resume { get; init; }

    /// <summary>
    /// Optional non-zero W3C trace identifier used to correlate this copy with caller-owned workflows.
    /// When omitted, the active activity trace identifier or a new identifier is used.
    /// </summary>
    public string? OperationId { get; init; }

    /// <summary>Optional callback that receives copy progress snapshots.</summary>
    public Action<DbaTableCopyProgress>? Progress { get; init; }
}
