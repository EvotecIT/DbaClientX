using DBAClientX.Metadata;
using Npgsql;

namespace DBAClientX;

public partial class PostgreSql
{
    internal async Task<IReadOnlyList<DbaColumnInfo>> GetTableCopyColumnsAsync(
        NpgsqlConnection connection,
        string schema,
        string table,
        CancellationToken cancellationToken)
    {
        using var command = new NpgsqlCommand(PostgreSqlColumnsQuery, connection)
        {
            CommandTimeout = CommandTimeout
        };
        command.Parameters.AddWithValue("@schema", schema);
        command.Parameters.AddWithValue("@table", table);

        var columns = new List<DbaColumnInfo>();
        using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            columns.Add(MapColumn(reader));
        }

        return columns;
    }
}
