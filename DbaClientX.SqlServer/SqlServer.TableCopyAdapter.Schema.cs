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
        using SqlConnection connection = CreateTableCopyConnection();
        await DBAClientX.Diagnostics.DbaClientXDiagnostics.OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);
        await ValidateSchemaAsync(connection, definition, page, options, cancellationToken).ConfigureAwait(false);
    }

    private async Task<IReadOnlyList<DbaColumnInfo>> ValidateSchemaAsync(
        SqlConnection connection,
        DbaTableCopyDefinition definition,
        DataTable page,
        DbaTableCopyOptions options,
        CancellationToken cancellationToken)
    {
        SqlServer.ValidateAutoCreateDestinationName(definition.DestinationName, GetEffectiveBulkInsertOptions(options, options.CheckpointId != null));
        string[] parts = DbaIdentifierPath.SplitSegments(definition.DestinationName).Select(DbaIdentifierPath.UnquoteSegment).ToArray();
        if (parts.Length is < 1 or > 3) throw new ArgumentException("SQL Server table-copy destinations support table, schema.table, or database.schema.table names.", nameof(definition));
        if (parts.Length == 3) connection.ChangeDatabase(parts[0]);
        using var server = new SqlServer { ConnectionOptions = _connectionOptions, CommandTimeout = CommandTimeout };
        IReadOnlyList<DbaColumnInfo> columns = await server.GetTableCopyColumnsAsync(connection, QuotePath(definition.DestinationName), cancellationToken).ConfigureAwait(false);
        if (columns.Count == 0 && ShouldWriteEmptyPage(definition) && !options.VerifyContent && options.CheckpointId == null) return columns;
        var generated = new HashSet<string>(columns.Where(IsGeneratedColumn).Select(static column => column.Name), StringComparer.OrdinalIgnoreCase);
        string[] projected = page.Columns.Cast<DataColumn>().Select(column =>
            _bulkInsertOptions?.ColumnMappings?.TryGetValue(column.ColumnName, out string? mapped) == true ? mapped : column.ColumnName)
            .Where(name => !generated.Contains(name)).ToArray();
        bool keepIdentity = ((GetEffectiveBulkInsertOptions(options)?.BulkCopyOptions ?? SqlBulkCopyOptions.Default) & SqlBulkCopyOptions.KeepIdentity) != 0;
        DbaTableCopySchemaValidator.Validate(definition.DestinationName, projected, columns, static name => name,
            options.VerifyContent || options.CheckpointId != null, keepIdentity);
        return columns;
    }

    private static bool IsGeneratedColumn(DbaColumnInfo column)
        => !string.IsNullOrWhiteSpace(column.GeneratedExpression) || !string.IsNullOrWhiteSpace(column.GeneratedKind) ||
           string.Equals(column.DataType, "rowversion", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(column.DataType, "timestamp", StringComparison.OrdinalIgnoreCase);

    // Computed and rowversion destination columns are produced by the server, so copies leave them out, as
    // Copy-DbaDbTableData does; the destination recomputes them.
    private readonly Dictionary<string, HashSet<string>> _generatedDestinationColumns = new(StringComparer.Ordinal);

    private async Task<HashSet<string>> GetGeneratedDestinationColumnsAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken)
    {
        lock (_generatedDestinationColumns)
        {
            if (_generatedDestinationColumns.TryGetValue(definition.DestinationName, out HashSet<string>? cached)) return cached;
        }

        var generated = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using (SqlConnection connection = CreateTableCopyConnection())
        {
            await DBAClientX.Diagnostics.DbaClientXDiagnostics.OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);
            IReadOnlyList<DbaTableCopyStreamColumn>? columns = await DescribeStreamColumnsAsync(connection, definition.DestinationName, cancellationToken).ConfigureAwait(false);
            foreach (DbaTableCopyStreamColumn column in columns ?? Array.Empty<DbaTableCopyStreamColumn>())
            {
                if (column.IsGenerated) generated.Add(column.Name);
            }
        }

        // A destination that does not exist yet is auto-created from the page and has no generated columns; the
        // next page looks again once it exists.
        if (generated.Count > 0 || _bulkInsertOptions?.AutoCreateTable != true)
        {
            lock (_generatedDestinationColumns) _generatedDestinationColumns[definition.DestinationName] = generated;
        }

        return generated;
    }

    private static DataTable? WithoutGeneratedColumns(DataTable page, HashSet<string> generated, IDictionary<string, string>? mappings)
    {
        if (generated.Count == 0) return null;
        string[] keep = page.Columns.Cast<DataColumn>()
            .Where(column => !generated.Contains(mappings != null && mappings.TryGetValue(column.ColumnName, out string? mapped) ? mapped : column.ColumnName))
            .Select(static column => column.ColumnName).ToArray();
        return keep.Length == page.Columns.Count ? null : page.DefaultView.ToTable(false, keep);
    }
}
