using DBAClientX.Metadata;
using MySqlConnector;

namespace DBAClientX;

public partial class MySql
{
    internal const string MySqlTableCopyColumnsQuery = @"
SELECT
    TABLE_SCHEMA AS schema_name,
    TABLE_NAME AS table_name,
    COLUMN_NAME AS column_name,
    COLUMN_TYPE AS data_type,
    ORDINAL_POSITION AS ordinal_position,
    CASE WHEN IS_NULLABLE = 'YES' THEN 1 ELSE 0 END AS is_nullable,
    CHARACTER_MAXIMUM_LENGTH AS max_length,
    NUMERIC_PRECISION AS numeric_precision,
    NUMERIC_SCALE AS numeric_scale,
    COLUMN_DEFAULT AS default_expression,
    CASE WHEN EXTRA LIKE '%auto_increment%' THEN 1 ELSE 0 END AS is_identity,
    CASE WHEN EXTRA LIKE '%auto_increment%' THEN 'AUTO_INCREMENT' ELSE NULL END AS identity_generation,
    NULLIF(GENERATION_EXPRESSION, '') AS generated_expression,
    CASE
        WHEN EXTRA LIKE '%STORED GENERATED%' THEN 'STORED'
        WHEN EXTRA LIKE '%VIRTUAL GENERATED%' THEN 'VIRTUAL'
        WHEN NULLIF(GENERATION_EXPRESSION, '') IS NOT NULL THEN 'GENERATED'
        ELSE NULL
    END AS generated_kind
FROM INFORMATION_SCHEMA.COLUMNS
WHERE (
        (@@lower_case_table_names = 0 AND BINARY TABLE_SCHEMA = BINARY @schema AND BINARY TABLE_NAME = BINARY @table)
        OR (@@lower_case_table_names <> 0 AND TABLE_SCHEMA = @schema AND TABLE_NAME = @table)
      )
ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;";

    internal async Task<IReadOnlyList<DbaColumnInfo>> GetTableCopyColumnsAsync(
        MySqlConnection connection,
        string database,
        string table,
        CancellationToken cancellationToken)
    {
        await using var command = new MySqlCommand(MySqlTableCopyColumnsQuery, connection)
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
