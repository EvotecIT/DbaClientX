namespace DBAClientX.DataMovement;

/// <summary>Opens a consistent source-read transaction spanning all table counts, pages, and validation.</summary>
public interface IDbaTableCopyReadSession
{
    /// <summary>Opens the configured source session. Dispose the returned lease after the copy, including on failure.</summary>
    Task<IDisposable?> OpenReadSessionAsync(CancellationToken cancellationToken = default);
}

/// <summary>Consistency of provider-backed table-copy source reads.</summary>
public enum DbaTableCopyReadConsistency
{
    /// <summary>The caller supplies an immutable snapshot or otherwise guarantees a stable source.</summary>
    CallerManaged,
    /// <summary>Uses the provider's transaction-stable snapshot. SQL Server requires ALLOW_SNAPSHOT_ISOLATION.</summary>
    Snapshot,
    /// <summary>Uses serializable isolation. Locking and conflict behavior follow the selected provider.</summary>
    Serializable
}
