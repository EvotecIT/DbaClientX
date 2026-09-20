using System.Data;
using MySqlConnector;

namespace DBAClientX;

public partial class MySql
{
    internal async Task WriteTableCopyRowsAsync(
        MySqlConnection connection,
        MySqlTransaction transaction,
        DataTable table,
        string destinationTable,
        int? batchSize,
        int? bulkCopyTimeout,
        CancellationToken cancellationToken)
    {
        var bulkCopy = TrackBulkCopy(CreateBulkCopy(connection, transaction), transaction);
        ConfigureBulkCopy(bulkCopy, table, destinationTable, bulkCopyTimeout);

        if (batchSize is > 0)
        {
            for (var offset = 0; offset < table.Rows.Count; offset += batchSize.Value)
            {
                await WriteToServerAsync(
                    bulkCopy,
                    EnumerateRows(table.Rows, offset, batchSize.Value),
                    table.Columns.Count,
                    cancellationToken).ConfigureAwait(false);
            }
        }
        else
        {
            await WriteToServerAsync(bulkCopy, table, cancellationToken).ConfigureAwait(false);
        }
    }

    private void ThrowIfBulkCopyWarnings(MySqlBulkCopyResult result, string? destinationTable, MySqlBulkCopy bulkCopy)
    {
        if (result.Warnings.Count == 0 || !_transactionalBulkCopies.TryGetValue(bulkCopy, out _)) return;
        throw new InvalidOperationException(
            $"MySQL bulk copy to '{destinationTable ?? "the destination"}' produced {result.Warnings.Count} conversion warning(s); the write was rejected to prevent silent data loss.");
    }
}
