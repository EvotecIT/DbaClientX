namespace DBAClientX.DataMovement;

/// <summary>
/// Allows destinations to request a schema-bearing empty page when the source has no rows.
/// </summary>
public interface IDbaTableCopyEmptyPageDestination
{
    /// <summary>
    /// Returns true when the destination needs <see cref="IDbaTableCopyDestination.WritePageAsync"/> for a zero-row page.
    /// </summary>
    bool ShouldWriteEmptyPage(DbaTableCopyDefinition definition);
}
