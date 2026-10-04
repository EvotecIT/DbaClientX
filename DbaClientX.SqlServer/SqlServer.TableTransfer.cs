using System.Data;
using DBAClientX.QueryBuilder;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public partial class SqlServer
{
    /// <summary>Streams one source table directly into native SQL bulk copy without materializing pages.</summary>
    /// <param name="request">Owned connections, table names, projection and explicit write policy.</param>
    /// <param name="cancellationToken">Caller cancellation token.</param>
    /// <returns>Native copied-row count and operation identifier, after successful transaction completion.</returns>
    /// <remarks>
    /// Existing destination rows are preserved. Default writes use one transaction; CommitEachBatch explicitly
    /// permits partial writes. This operation never retries, resumes, clears tables or verifies content. Source
    /// ReadCommitted permits concurrent changes; Snapshot and Serializable require their native capabilities.
    /// Tables must be local user tables, not synonyms or views. Source and destination cannot be the same native table.
    /// </remarks>
    public static async Task<SqlServerBulkInsertResult> TransferTableAsync(
        SqlServerTableTransferRequest request,
        CancellationToken cancellationToken = default)
    {
        if (request == null) throw new ArgumentNullException(nameof(request));
        // Freeze mutable configuration before the first asynchronous operation.
        string sourceConnection = TransferConnectionString(request.SourceConnectionString);
        string destinationConnection = TransferConnectionString(request.DestinationConnectionString);
        string sourceTable = SqlServerDestinationTable.Parse(request.SourceTable).QuotedFullName;
        string destinationTable = SqlServerDestinationTable.Parse(request.DestinationTable).QuotedFullName;
        int commandTimeout = request.CommandTimeout;
        int bulkTimeout = request.BulkCopyTimeout;
        int batchSize = request.BatchSize;
        bool perBatch = request.CommitEachBatch;
        IsolationLevel isolation = request.SourceIsolationLevel;
        if (commandTimeout < 0) throw new ArgumentOutOfRangeException(nameof(request.CommandTimeout));
        if (isolation is not (IsolationLevel.ReadCommitted or IsolationLevel.Snapshot or IsolationLevel.Serializable))
            throw new ArgumentOutOfRangeException(nameof(request.SourceIsolationLevel), "Use ReadCommitted, Snapshot or Serializable.");
        SqlServerBulkInsertOptions bulkOptions = TransferBulkOptions(request.BulkOptions, perBatch);
        ValidateBulkInsertSettings(batchSize, bulkTimeout, bulkOptions);
        var query = new Query().FromRaw(sourceTable);
        if (request.SourceColumns != null)
        {
            string[] columns = request.SourceColumns.ToArray();
            if (columns.Length == 0) throw new ArgumentException("Source projection must contain at least one column.", nameof(request));
            query.SelectRaw(columns.Select(column => SqlIdentifier.Quote(SqlDialect.SqlServer, column)).ToArray());
        }
        var compiler = new QueryCompiler(SqlDialect.SqlServer);
        string schemaSelect = compiler.Compile(query.Limit(0));
        using var source = new SqlServer { CommandTimeout = commandTimeout, ConnectionOptions = TransferConnectionOptions(request.SourceConnectionOptions) };
        using var destination = new SqlServer { CommandTimeout = commandTimeout, ConnectionOptions = TransferConnectionOptions(request.DestinationConnectionOptions) };
        cancellationToken.ThrowIfCancellationRequested();

        // Complete the read transaction inside the destination transaction, so source cleanup/commit failure
        // cannot occur after the default destination commit.
        Task<SqlServerBulkInsertResult> CopyRowsAsync(bool destinationTransaction, CancellationToken token)
            => source.RunInTransactionAsync(sourceConnection, async (readerClient, readToken) =>
            {
                TransferTableIdentity sourceIdentity = await ReadTransferIdentityAsync(readerClient, sourceConnection, sourceTable, true, readToken).ConfigureAwait(false);
                TransferTableIdentity targetIdentity = await ReadTransferIdentityAsync(destination, destinationConnection, destinationTable, destinationTransaction, readToken).ConfigureAwait(false);
                if (!sourceIdentity.ObjectId.HasValue)
                    throw new InvalidOperationException("Source must be a visible local user table.");
                if (!targetIdentity.ObjectId.HasValue && !bulkOptions.AutoCreateTable)
                    throw new InvalidOperationException("Destination must be a visible local user table, or AutoCreateTable must be explicitly enabled.");
                if (sourceIdentity.ObjectId == targetIdentity.ObjectId && sourceIdentity.DatabaseId == targetIdentity.DatabaseId &&
                    string.Equals(sourceIdentity.ServerName, targetIdentity.ServerName, StringComparison.OrdinalIgnoreCase))
                    throw new InvalidOperationException("Refusing to stream a native table into itself.");
                string select = await TransferProjectionAsync(readerClient, sourceConnection, sourceTable, schemaSelect,
                    destination, destinationConnection, targetIdentity.ObjectId, destinationTransaction, bulkOptions, readToken).ConfigureAwait(false);
                await using var reader = await readerClient.QueryReaderAsync(sourceConnection, select, useTransaction: true, cancellationToken: readToken).ConfigureAwait(false);
                SqlServerBulkInsertResult result = await destination.BulkInsertWithResultAsync(destinationConnection, reader, destinationTable,
                    bulkOptions, useTransaction: destinationTransaction, batchSize: batchSize, bulkCopyTimeout: bulkTimeout, cancellationToken: readToken).ConfigureAwait(false);
                readToken.ThrowIfCancellationRequested();
                return result;
            }, isolation, token);

        return perBatch
            ? await CopyRowsAsync(false, cancellationToken).ConfigureAwait(false)
            : await destination.RunInTransactionAsync(destinationConnection, (_, token) => CopyRowsAsync(true, token), cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    private static string TransferConnectionString(string value)
    {
        ValidateConnectionString(value);
        return new SqlConnectionStringBuilder(value) { Enlist = false, Pooling = false }.ConnectionString;
    }

    private static SqlServerConnectionOptions TransferConnectionOptions(SqlServerConnectionOptions? options)
    {
        if (options?.CompatibilityProfile == SqlServerCompatibilityProfile.FabricWarehouse)
            throw new NotSupportedException("Table streaming with owned transactions is a native SQL Server workflow.");
        Func<string, SqlConnection>? factory = options?.ConnectionFactory;
        return new SqlServerConnectionOptions
        {
            AccessTokenCallback = options?.AccessTokenCallback,
            ConnectionFactory = factory == null ? null : value =>
            {
                SqlConnection connection = factory(value) ?? throw new InvalidOperationException("The SQL Server connection factory returned null.");
                try
                {
                    if (connection.State != ConnectionState.Closed ||
                        !new SqlConnectionStringBuilder(value).EquivalentTo(new SqlConnectionStringBuilder(connection.ConnectionString)))
                        throw new InvalidOperationException("Transfer factories must return a closed connection with the supplied connection string.");
                    return connection;
                }
                catch { connection.Dispose(); throw; }
            }
        };
    }

    private static SqlServerBulkInsertOptions TransferBulkOptions(SqlServerBulkInsertOptions? options, bool perBatch)
    {
        SqlBulkCopyOptions flags = options?.BulkCopyOptions ?? SqlBulkCopyOptions.CheckConstraints;
        if (!perBatch && (flags & SqlBulkCopyOptions.UseInternalTransaction) != 0)
            throw new ArgumentException("Use CommitEachBatch to opt into partial commits instead of UseInternalTransaction.", nameof(options));
        return new SqlServerBulkInsertOptions
        {
            BulkCopyOptions = perBatch ? flags | SqlBulkCopyOptions.UseInternalTransaction : flags,
            AutoCreateTable = options?.AutoCreateTable ?? false,
            ColumnMappings = options?.ColumnMappings == null ? null : new Dictionary<string, string>(options.ColumnMappings, GetComparer(options.ColumnMappings)),
            NotifyAfter = options?.NotifyAfter,
            RowsCopied = options?.RowsCopied
        };
    }

    private static async Task<TransferTableIdentity> ReadTransferIdentityAsync(SqlServer client, string connectionString, string table, bool transaction, CancellationToken token)
    {
        await using var reader = await client.QueryReaderAsync(connectionString,
            "SELECT CONVERT(nvarchar(128),SERVERPROPERTY('ServerName')),CONVERT(int,DB_ID()),OBJECT_ID(@table,N'U')",
            new Dictionary<string, object?> { ["@table"] = table }, useTransaction: transaction, cancellationToken: token).ConfigureAwait(false);
        if (!await reader.ReadAsync(token).ConfigureAwait(false) || reader.IsDBNull(0))
            throw new InvalidOperationException("Native server/database identity is unavailable for table transfer.");
        string server = reader.GetString(0);
        if (reader.IsDBNull(1)) throw new InvalidOperationException("Native database identity is unavailable for table transfer.");
        int database = reader.GetInt32(1);
        int? objectId = reader.IsDBNull(2) ? null : reader.GetInt32(2);
        return new TransferTableIdentity(server, database, objectId);
    }

    private static async Task<string> TransferProjectionAsync(SqlServer source, string sourceConnection, string sourceTable, string schemaSelect,
        SqlServer destination, string destinationConnection, int? destinationObjectId, bool destinationTransaction, SqlServerBulkInsertOptions options, CancellationToken token)
    {
        IReadOnlyList<SqlServerBulkSourceColumn> columns;
        await using (var schemaReader = await source.QueryReaderAsync(sourceConnection, schemaSelect, useTransaction: true, cancellationToken: token).ConfigureAwait(false))
        {
            var names = new HashSet<string>(StringComparer.Ordinal);
            for (int index = 0; index < schemaReader.FieldCount; index++)
                if (!names.Add(schemaReader.GetName(index))) throw new ArgumentException("Source projection cannot repeat a column.");
            columns = GetValidatedReaderColumns(schemaReader, options.ColumnMappings);
        }
        IEnumerable<SqlServerBulkSourceColumn> ordered = columns;
        if (destinationObjectId.HasValue)
        {
            // SqlBulkCopy consumes source values in destination ordinal order. Ask the native catalog to match
            // destination names under its collation, then order the SELECT rather than buffering/reinterpreting rows.
            if (columns.Count > 2000) throw new NotSupportedException("Use an explicit projection of at most 2000 columns for native metadata matching.");
            var parameters = new Dictionary<string, object?> { ["@objectId"] = destinationObjectId.Value };
            var values = new List<string>(columns.Count);
            for (int index = 0; index < columns.Count; index++)
            {
                string parameter = "@column" + index.ToString(System.Globalization.CultureInfo.InvariantCulture);
                parameters[parameter] = columns[index].DestinationName;
                values.Add("(" + index.ToString(System.Globalization.CultureInfo.InvariantCulture) + "," + parameter + ")");
            }
            var ordinals = new List<int>(columns.Count);
            await using (var destinationReader = await destination.QueryReaderAsync(destinationConnection,
                "SELECT m.Ordinal FROM sys.columns c JOIN (VALUES " + string.Join(",", values) +
                ") m(Ordinal,Name) ON c.name=m.Name COLLATE DATABASE_DEFAULT WHERE c.object_id=@objectId ORDER BY c.column_id",
                parameters, useTransaction: destinationTransaction, cancellationToken: token).ConfigureAwait(false))
            {
                while (await destinationReader.ReadAsync(token).ConfigureAwait(false)) ordinals.Add(destinationReader.GetInt32(0));
            }
            if (ordinals.Count != columns.Count || ordinals.Distinct().Count() != columns.Count)
                throw new ArgumentException("Every projected source column must map to one visible destination column.");
            ordered = ordinals.Select(index => columns[index]);
        }
        return new QueryCompiler(SqlDialect.SqlServer).Compile(new Query().FromRaw(sourceTable)
            .SelectRaw(ordered.Select(column => SqlIdentifier.Quote(SqlDialect.SqlServer, column.SourceName)).ToArray()));
    }

    private sealed record TransferTableIdentity(string ServerName, int DatabaseId, int? ObjectId);
}
