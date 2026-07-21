namespace DBAClientX.DataMovement;

/// <summary>Allows a destination adapter to reject destructive clear operations before source preflight begins.</summary>
public interface IDbaTableCopyClearSafetyValidator
{
    /// <summary>Validates that clearing all destination definitions cannot remove data needed by the source.</summary>
    void ValidateClearOperation(IDbaTableCopySource source, IReadOnlyList<DbaTableCopyDefinition> definitions);
}
