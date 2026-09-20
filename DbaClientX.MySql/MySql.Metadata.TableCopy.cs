using DBAClientX.Metadata;
using MySqlConnector;

namespace DBAClientX;

public partial class MySql
{
    internal async Task<IReadOnlyList<DbaColumnInfo>> GetTableCopyColumnsAsync(
        MySqlConnection connection,
        string database,
        string table,
        CancellationToken cancellationToken)
    {
        await using var command = new MySqlCommand(MySqlColumnsQuery, connection)
        {
            CommandTimeout = CommandTimeout
        };
        command.Parameters.AddWithValue("@schema", database);
        command.Parameters.AddWithValue("@table", table);

        var columns = new List<DbaColumnInfo>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            columns.Add(MapColumn(reader));
        }

        return columns;
    }
}
