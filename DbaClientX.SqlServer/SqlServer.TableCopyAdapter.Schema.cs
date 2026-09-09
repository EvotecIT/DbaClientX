using System.Data;
using DBAClientX.DataMovement;
using DBAClientX.Metadata;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public sealed partial class SqlServerTableCopyAdapter : IDbaTableCopySchemaPreflightDestination
{
    /// <inheritdoc />
    public async Task ValidateSchemaAsync(DbaTableCopyDefinition definition, DataTable page, DbaTableCopyOptions options, CancellationToken cancellationToken)
    {
        SqlServer.ValidateAutoCreateDestinationName(definition.DestinationName, GetEffectiveBulkInsertOptions(options, options.CheckpointId != null));
        string[] parts = DbaIdentifierPath.SplitSegments(definition.DestinationName).Select(DbaIdentifierPath.UnquoteSegment).ToArray();
        if (parts.Length is < 1 or > 3) throw new ArgumentException("SQL Server table-copy destinations support table, schema.table, or database.schema.table names.", nameof(definition));
        using SqlConnection connection = CreateTableCopyConnection();
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        if (parts.Length == 3) connection.ChangeDatabase(parts[0]);
        using var server = new SqlServer { ConnectionOptions = _connectionOptions, CommandTimeout = CommandTimeout };
        IReadOnlyList<DbaColumnInfo> columns = await server.GetTableCopyColumnsAsync(connection, QuotePath(definition.DestinationName), cancellationToken).ConfigureAwait(false);
        if (columns.Count == 0 && ShouldWriteEmptyPage(definition) && !options.VerifyContent && options.CheckpointId == null) return;
        string[] projected = page.Columns.Cast<DataColumn>().Select(column =>
            _bulkInsertOptions?.ColumnMappings?.TryGetValue(column.ColumnName, out string? mapped) == true ? mapped : column.ColumnName).ToArray();
        bool keepIdentity = ((GetEffectiveBulkInsertOptions(options)?.BulkCopyOptions ?? SqlBulkCopyOptions.Default) & SqlBulkCopyOptions.KeepIdentity) != 0;
        DbaTableCopySchemaValidator.Validate(definition.DestinationName, projected, columns, static name => name,
            options.VerifyContent || options.CheckpointId != null, keepIdentity);
    }
}
