using System.Data;

namespace DBAClientX.DataMovement;

/// <summary>Validates a transformed source projection against the destination schema before destructive writes.</summary>
public interface IDbaTableCopySchemaPreflightDestination
{
    /// <summary>Rejects missing, generated, or unsupplied required destination columns before clearing data.</summary>
    Task ValidateSchemaAsync(DbaTableCopyDefinition definition, DataTable page, DbaTableCopyOptions options, CancellationToken cancellationToken);
}
