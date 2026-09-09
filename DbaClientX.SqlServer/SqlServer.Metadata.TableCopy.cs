using DBAClientX.Metadata;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public partial class SqlServer
{
    internal async Task<IReadOnlyList<DbaColumnInfo>> GetTableCopyColumnsAsync(SqlConnection connection, string tableName, CancellationToken cancellationToken)
    {
        string schema;
        string table;
        using (SqlCommand resolve = connection.CreateCommand())
        {
            resolve.CommandText = "SELECT OBJECT_SCHEMA_NAME(OBJECT_ID(@name)), OBJECT_NAME(OBJECT_ID(@name))";
            resolve.Parameters.AddWithValue("@name", tableName);
            ApplyCommandTimeout(resolve);
            using SqlDataReader reader = await resolve.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false) || reader.IsDBNull(0)) return Array.Empty<DbaColumnInfo>();
            schema = reader.GetString(0);
            table = reader.GetString(1);
        }
        return await ExecuteMappedQueryAsync(connection, null, SqlServerColumnsQuery, MapColumn,
            parameters: new Dictionary<string, object?> { ["@schema"] = schema, ["@table"] = table }, cancellationToken: cancellationToken).ConfigureAwait(false);
    }
}
