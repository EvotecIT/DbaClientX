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
        var bulkCopy = CreateBulkCopy(connection, transaction);
        bulkCopy.DestinationTableName = destinationTable;
        if (bulkCopyTimeout.HasValue)
        {
            bulkCopy.BulkCopyTimeout = bulkCopyTimeout.Value;
        }

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
}
