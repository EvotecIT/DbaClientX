using System.Data;

namespace DBAClientX.DataMovement;

/// <summary>
/// Allows destination adapters to validate a transformed page before destructive destination actions run.
/// </summary>
public interface IDbaTableCopyPagePreflightDestination
{
    /// <summary>
    /// Validates a transformed destination page before any destination rows are cleared.
    /// </summary>
    /// <param name="definition">The table copy definition being preflighted.</param>
    /// <param name="page">The transformed destination page that would be written.</param>
    void ValidatePage(DbaTableCopyDefinition definition, DataTable page);
}
