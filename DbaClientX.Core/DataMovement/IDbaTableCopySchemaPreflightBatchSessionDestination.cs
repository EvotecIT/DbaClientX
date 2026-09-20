using System.Data;

namespace DBAClientX.DataMovement;

/// <summary>Validates a related set of projected tables in one rollback-only destination transaction.</summary>
public interface IDbaTableCopySchemaPreflightBatchSessionDestination
{
    /// <summary>
    /// Opens a rollback-only validation session and clears the supplied destinations in reverse order.
    /// The caller then validates every page for each definition in forward dependency order.
    /// </summary>
    Task<IDbaTableCopySchemaPreflightBatchSession> OpenSchemaPreflightBatchSessionAsync(
        IReadOnlyList<DbaTableCopyDefinition> definitions,
        IReadOnlyList<DataTable?> firstPages,
        DbaTableCopyOptions options,
        CancellationToken cancellationToken);
}

/// <summary>A rollback-only validation session spanning a related set of destination tables.</summary>
public interface IDbaTableCopySchemaPreflightBatchSession : IAsyncDisposable
{
    /// <summary>Validates another projected page for the definition at <paramref name="definitionIndex"/>.</summary>
    Task ValidatePageAsync(int definitionIndex, DataTable page, CancellationToken cancellationToken);
}
