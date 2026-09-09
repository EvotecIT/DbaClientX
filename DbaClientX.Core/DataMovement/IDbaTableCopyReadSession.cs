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
    /// <summary>Uses SQL Server snapshot isolation; ALLOW_SNAPSHOT_ISOLATION must already be enabled.</summary>
    Snapshot,
    /// <summary>Holds serializable source read locks. Suitable for an offline source; blocks concurrent writers until copying finishes.</summary>
    Serializable
}
