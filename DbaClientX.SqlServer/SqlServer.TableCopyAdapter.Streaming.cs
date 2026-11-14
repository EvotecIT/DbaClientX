using System.Data;
using DBAClientX.DataMovement;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public sealed partial class SqlServerTableCopyAdapter : IDbaTableCopyStreamSource, IDbaTableCopyStreamDestination
{
    // Describes exactly the columns SELECT * returns, including views, synonyms and three-part names.
    private const string StreamColumnsQuery =
        "SELECT name, CONVERT(int, COLLATIONPROPERTY(collation_name, 'LCID')), error_number " +
        "FROM sys.dm_exec_describe_first_result_set(@query, NULL, 0) WHERE ISNULL(is_hidden, 0) = 0 ORDER BY column_ordinal";

    bool IDbaTableCopyStreamSource.CanStream(DbaTableCopyDefinition definition)
        => _connectionOptions.CompatibilityProfile == SqlServerCompatibilityProfile.Default &&
           definition.SourceOptions?.HasDeduplication != true &&
           !HasSingleConnectionPool();

    async Task<IReadOnlyList<DbaTableCopyStreamColumn>?> IDbaTableCopyStreamSource.DescribeStreamAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken)
    {
        if (_readConnection != null)
        {
            return await DescribeStreamColumnsAsync(_readConnection, definition.SourceName, cancellationToken).ConfigureAwait(false);
        }

        using SqlConnection connection = CreateTableCopyConnection();
        await DBAClientX.Diagnostics.DbaClientXDiagnostics.OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);
        return await DescribeStreamColumnsAsync(connection, definition.SourceName, cancellationToken).ConfigureAwait(false);
    }

    async Task<IDataReader> IDbaTableCopyStreamSource.OpenStreamAsync(DbaTableCopyDefinition definition, IReadOnlyList<string> columns, CancellationToken cancellationToken)
    {
        string query = BuildTableReadQuery(definition, string.Join(", ", columns.Select(static column => "[" + column.Replace("]", "]]") + "]")));
        if (_readConnection != null)
        {
            SqlCommand command = CreateSourceCommand(_readConnection, query);
            try
            {
                SqlDataReader reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken).ConfigureAwait(false);
                return new DbaDataReader(reader, command);
            }
            catch
            {
                command.Dispose();
                throw;
            }
        }

        using var sqlServer = new SqlServer { ConnectionOptions = _connectionOptions, CommandTimeout = CommandTimeout };
        return await sqlServer.QueryReaderAsync(ConnectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    // Auto-created destinations keep the page path so their schema still comes from the DataTable shape.
    bool IDbaTableCopyStreamDestination.CanStream(DbaTableCopyDefinition definition, DbaTableCopyOptions options)
        => _connectionOptions.CompatibilityProfile == SqlServerCompatibilityProfile.Default &&
           _bulkInsertOptions?.AutoCreateTable != true &&
           !HasSingleConnectionPool();

    // A streamed copy holds the source connection while it opens the destination one. A pool limited to a single
    // connection, which both adapters share when they use the same connection string, would wait for itself, so
    // such copies keep the page path, which releases the source connection before each write.
    private bool HasSingleConnectionPool()
    {
        var builder = new SqlConnectionStringBuilder(ConnectionString);
        return builder.Pooling && builder.MaxPoolSize < 2;
    }

    // SqlBulkCopy reads a native SqlDataReader in destination column order and rejects a character destination whose
    // collation locale differs from the source column's, including a non-character source. Order the source projection to match the destination, and
    // leave locale mismatches and unmatched columns to the page path, which copies through DataTable values.
    async Task<IReadOnlyList<string>?> IDbaTableCopyStreamDestination.OrderStreamColumnsAsync(
        DbaTableCopyDefinition definition,
        DbaTableCopyOptions options,
        IReadOnlyList<DbaTableCopyStreamColumn> sourceColumns,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<DbaTableCopyStreamColumn>? destinationColumns;
        using (SqlConnection connection = CreateTableCopyConnection())
        {
            await DBAClientX.Diagnostics.DbaClientXDiagnostics.OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);
            destinationColumns = await DescribeStreamColumnsAsync(connection, definition.DestinationName, cancellationToken).ConfigureAwait(false);
        }

        if (destinationColumns == null)
        {
            return null;
        }

        var destinationOrdinals = new Dictionary<string, int>(StringComparer.Ordinal);
        for (int ordinal = 0; ordinal < destinationColumns.Count; ordinal++)
        {
            destinationOrdinals[destinationColumns[ordinal].Name] = ordinal;
        }

        IDictionary<string, string>? mappings = GetEffectiveBulkInsertOptions(options)?.ColumnMappings;
        var ordered = new SortedList<int, string>(sourceColumns.Count);
        foreach (DbaTableCopyStreamColumn source in sourceColumns)
        {
            string destinationName = mappings != null && mappings.TryGetValue(source.Name, out string? mapped) ? mapped : source.Name;
            if (destinationName.Length >= 2 && destinationName[0] == '[' && destinationName[destinationName.Length - 1] == ']')
            {
                destinationName = destinationName.Substring(1, destinationName.Length - 2);
            }

            if (!destinationOrdinals.TryGetValue(destinationName, out int ordinal) || ordered.ContainsKey(ordinal))
            {
                return null;
            }

            int? destinationLcid = destinationColumns[ordinal].CollationLcid;
            // A non-character source reports locale 0, which SqlClient also rejects for a character destination.
            if (destinationLcid.HasValue && source.CollationLcid != destinationLcid)
            {
                return null;
            }

            ordered.Add(ordinal, source.Name);
        }

        return ordered.Values.ToArray();
    }

    async Task<long> IDbaTableCopyStreamDestination.WriteStreamAsync(
        DbaTableCopyDefinition definition,
        IDataReader reader,
        DbaTableCopyOptions options,
        Action<long>? rowsCopied,
        CancellationToken cancellationToken)
    {
        SqlServerBulkInsertOptions? bulkOptions = GetEffectiveBulkInsertOptions(options);
        if (rowsCopied != null)
        {
            Action<long>? configured = bulkOptions?.RowsCopied;
            bulkOptions = new SqlServerBulkInsertOptions
            {
                BulkCopyOptions = bulkOptions?.BulkCopyOptions ?? SqlBulkCopyOptions.Default,
                AutoCreateTable = bulkOptions?.AutoCreateTable ?? false,
                ColumnMappings = bulkOptions?.ColumnMappings,
                NotifyAfter = bulkOptions?.NotifyAfter,
                RowsCopied = configured == null ? rowsCopied : rows => { configured(rows); rowsCopied(rows); }
            };
        }

        using var sqlServer = new SqlServer { ConnectionOptions = _connectionOptions, CommandTimeout = CommandTimeout };
        SqlServerBulkInsertResult result = await sqlServer.BulkInsertWithResultAsync(
                ConnectionString,
                reader,
                NormalizeQuotedBulkDestinationTableName(definition.DestinationName),
                bulkOptions,
                batchSize: options.BatchSize,
                bulkCopyTimeout: options.BulkCopyTimeout,
                cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        return result.RowsCopied;
    }

    private async Task<IReadOnlyList<DbaTableCopyStreamColumn>?> DescribeStreamColumnsAsync(SqlConnection connection, string tableName, CancellationToken cancellationToken)
    {
        using SqlCommand command = CreateSourceCommand(connection, StreamColumnsQuery);
        command.Parameters.Add("@query", SqlDbType.NVarChar, -1).Value = "SELECT * FROM " + QuotePath(tableName);
        var columns = new List<DbaTableCopyStreamColumn>();
        using SqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            // A missing or undescribable object returns an error row; the page path then reports the real failure.
            if (reader.IsDBNull(0) || !reader.IsDBNull(2))
            {
                return null;
            }

            columns.Add(new DbaTableCopyStreamColumn(reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetInt32(1)));
        }

        return columns.Count == 0 ? null : columns;
    }
}
