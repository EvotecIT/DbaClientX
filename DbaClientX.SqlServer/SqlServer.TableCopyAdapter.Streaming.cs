using System.Data;
using DBAClientX.DataMovement;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public sealed partial class SqlServerTableCopyAdapter : IDbaTableCopyStreamSource, IDbaTableCopyStreamDestination
{
    bool IDbaTableCopyStreamSource.CanStream(DbaTableCopyDefinition definition)
        => _connectionOptions.CompatibilityProfile == SqlServerCompatibilityProfile.Default &&
           definition.SourceOptions?.HasDeduplication != true;

    async Task<IDataReader> IDbaTableCopyStreamSource.OpenStreamAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken)
    {
        string query = BuildTableReadQuery(definition);
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
           _bulkInsertOptions?.AutoCreateTable != true;

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
}
