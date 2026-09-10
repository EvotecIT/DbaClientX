namespace DBAClientX.DataMovement;

/// <summary>Durable progress committed in the destination database in the same transaction as a copied page.</summary>
public sealed record DbaTableCopyCheckpoint
{
    /// <summary>Caller-owned identifier retained across resumes.</summary>
    public string CopyId { get; init; } = string.Empty;
    /// <summary>Fingerprint binding the checkpoint to the table's copy contract.</summary>
    public string DefinitionFingerprint { get; init; } = string.Empty;
    /// <summary>Number of rows in the validated source projection.</summary>
    public long SourceRows { get; init; }
    /// <summary>SHA-256 multiset checksum of all projected source rows.</summary>
    public string SourceContentHash { get; init; } = string.Empty;
    /// <summary>Number of rows committed by this copy.</summary>
    public long CopiedRows { get; init; }
    /// <summary>Provider-owned continuation token following the last committed row. Contains key values; keep private.</summary>
    public string? ContinuationToken { get; init; }
    /// <summary>Incremental checksum of committed rows.</summary>
    public string CopiedContentHash { get; init; } = string.Empty;
    /// <summary>True only after the entire destination table passed content verification.</summary>
    public bool Completed { get; init; }
}
