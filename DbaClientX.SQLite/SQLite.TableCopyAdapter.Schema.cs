using System.Data;
using DBAClientX.DataMovement;
using DBAClientX.Metadata;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

public sealed partial class SQLiteTableCopyAdapter : IDbaTableCopySchemaPreflightDestination
{
    /// <inheritdoc />
    public async Task ValidateSchemaAsync(DbaTableCopyDefinition definition, DataTable page, DbaTableCopyOptions options, CancellationToken cancellationToken)
    {
        string[] parts = DbaIdentifierPath.SplitSegments(definition.DestinationName).Select(DbaIdentifierPath.UnquoteSegment).ToArray();
        if (parts.Length is < 1 or > 2) throw new ArgumentException("SQLite table-copy destinations support table or schema.table names.", nameof(definition));
        string schema = parts.Length == 2 ? parts[0] : "main";
        if (string.Equals(schema, "main", StringComparison.OrdinalIgnoreCase)) schema = "main";
        using var connection = new SqliteConnection(ResolveSQLiteConnectionString());
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        using var sqlite = new SQLite { CommandTimeout = CommandTimeout };
        IReadOnlyList<DbaColumnInfo> columns = await sqlite.GetTableCopyColumnsAsync(connection, schema, parts[parts.Length - 1], cancellationToken).ConfigureAwait(false);
        DbaTableCopySchemaValidator.Validate(definition.DestinationName, page.Columns.Cast<DataColumn>().Select(column => column.ColumnName).ToArray(),
            columns, DbaIdentifierPath.NormalizeSqliteIdentifier, requirePreservedIdentity: false, keepIdentity: true);
    }
}
