using System.Data;

namespace DBAClientX.DataMovement;

/// <summary>Validates every projected source page against one rollback-only destination transaction.</summary>
public interface IDbaTableCopySchemaPreflightSessionDestination
{
    /// <summary>Opens a rollback-only validation session and validates the first projected page.</summary>
    Task<IDbaTableCopySchemaPreflightSession> OpenSchemaPreflightSessionAsync(
        DbaTableCopyDefinition definition,
        DataTable firstPage,
        DbaTableCopyOptions options,
        CancellationToken cancellationToken);
}

/// <summary>A rollback-only destination validation session spanning all projected source pages.</summary>
public interface IDbaTableCopySchemaPreflightSession : IAsyncDisposable
{
    /// <summary>Validates another projected page in the same rollback-only destination transaction.</summary>
    Task ValidatePageAsync(DataTable page, CancellationToken cancellationToken);
}
