namespace DBAClientX.DataMovement;

/// <summary>Validates source values and schema shapes against a destination provider before any rows are written.</summary>
public interface IDbaTableCopyDestinationCompatibilitySource
{
    /// <summary>Rejects provider-specific source shapes that cannot be represented by the destination provider.</summary>
    Task ValidateDestinationCompatibilityAsync(
        DbaTableCopyProvider destinationProvider,
        IReadOnlyList<DbaTableCopyDefinition> definitions,
        CancellationToken cancellationToken);
}
