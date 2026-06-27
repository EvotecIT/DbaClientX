using System.Data;

namespace DBAClientX.DataMovement;

/// <summary>
/// Reads row counts and pages for reusable table-data copy operations.
/// </summary>
public interface IDbaTableCopySource
{
    /// <summary>Counts effective source rows for a copy definition, or returns null when counting is unavailable.</summary>
    Task<long?> CountRowsAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken = default);

    /// <summary>Reads one source page as a <see cref="DataTable"/>.</summary>
    Task<DataTable> ReadPageAsync(DbaTableCopyPageRequest request, CancellationToken cancellationToken = default);
}
