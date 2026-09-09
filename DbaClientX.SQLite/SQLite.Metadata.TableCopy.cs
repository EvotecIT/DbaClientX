using DBAClientX.Metadata;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

public partial class SQLite
{
    internal async Task<IReadOnlyList<DbaColumnInfo>> GetTableCopyColumnsAsync(SqliteConnection connection, string schema, string table, CancellationToken cancellationToken)
    {
        using SqliteCommand resolve = connection.CreateCommand();
        resolve.CommandText = "SELECT name FROM [" + schema.Replace("]", "]]") + "].sqlite_master WHERE type='table' AND name=@name COLLATE NOCASE";
        resolve.Parameters.AddWithValue("@name", table);
        ApplyCommandTimeout(resolve);
        object? actualName = await resolve.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        if (actualName is not string actual) return Array.Empty<DbaColumnInfo>();
        return await ExecuteMappedQueryAsync(connection, null, SQLiteColumnsQuery, MapColumn,
            parameters: new Dictionary<string, object?> { ["@schema"] = schema, ["@table"] = actual }, cancellationToken: cancellationToken).ConfigureAwait(false);
    }
}
