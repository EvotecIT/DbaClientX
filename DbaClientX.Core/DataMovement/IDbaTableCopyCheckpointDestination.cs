using System.Data;

namespace DBAClientX.DataMovement;

/// <summary>Destination support for atomic page writes and durable, compare-and-swap checkpoints.</summary>
public interface IDbaTableCopyCheckpointDestination
{
    /// <summary>Whether this destination implements atomic checkpoint storage.</summary>
    bool SupportsAtomicCheckpoints { get; }
    /// <summary>Reads the checkpoint bound to a destination table, or null if no copy has initialized it.</summary>
    Task<DbaTableCopyCheckpoint?> ReadCheckpointAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken = default);
    /// <summary>Atomically initializes an empty destination, or explicitly clears it and starts a new copy.</summary>
    Task InitializeCheckpointAsync(DbaTableCopyDefinition definition, DbaTableCopyCheckpoint checkpoint, bool clearDestination, CancellationToken cancellationToken = default);
    /// <summary>Commits rows and the next checkpoint atomically, only if the stored checkpoint still matches expected.</summary>
    Task CommitPageAsync(DbaTableCopyDefinition definition, DataTable page, DbaTableCopyOptions options, DbaTableCopyCheckpoint expected, DbaTableCopyCheckpoint next, CancellationToken cancellationToken = default);
}
