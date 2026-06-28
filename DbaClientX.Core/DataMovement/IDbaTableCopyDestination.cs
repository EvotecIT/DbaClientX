using System.Data;

namespace DBAClientX.DataMovement;

/// <summary>
/// Writes pages and verifies row counts for reusable table-data copy operations.
/// </summary>
public interface IDbaTableCopyDestination
{
    /// <summary>Clears the destination table before copying when requested.</summary>
    Task ClearAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken = default);

    /// <summary>Writes one page to the destination table.</summary>
    Task WritePageAsync(DbaTableCopyDefinition definition, DataTable page, DbaTableCopyOptions options, CancellationToken cancellationToken = default);

    /// <summary>Counts destination rows after a copy, or returns null when counting is unavailable.</summary>
    Task<long?> CountRowsAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken = default);
}
