using System.Data;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

public partial class SQLite
{
    /// <summary>Uses the shared prepared bulk writer inside a caller-owned transaction without committing it.</summary>
    internal async Task WriteBulkRowsAsync(SqliteConnection connection, SqliteTransaction? transaction, DataTable table, string destinationTable, int? batchSize, CancellationToken cancellationToken)
    {
        int totalRows = table.Rows.Count;
        if (totalRows == 0) return;
        DataColumn[] columns = GetColumns(table);
        int rowsPerBatch = ResolveRowsPerBatch(totalRows, batchSize, columns.Length);
        SqliteCommand? command = null;
        int preparedRowsPerBatch = 0;
        try
        {
            for (int offset = 0; offset < totalRows; offset += rowsPerBatch)
            {
                cancellationToken.ThrowIfCancellationRequested();
                int currentRows = Math.Min(rowsPerBatch, totalRows - offset);
                if (command == null || preparedRowsPerBatch != currentRows)
                {
                    command?.Dispose();
                    command = CreatePreparedBulkInsertCommand(connection, transaction, destinationTable, columns, currentRows);
                    preparedRowsPerBatch = currentRows;
                }
                ApplyBatchValues(command, columns, table, offset, currentRows);
                await ExecuteBulkInsertCommandAsync(command, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            command?.Dispose();
        }
    }
}
