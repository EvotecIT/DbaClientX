using System.Data;
using System.Data.Common;
using DBAClientX.DataMovement;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public sealed partial class SqlServerTableCopyAdapter
{
    /// <inheritdoc />
    public override bool SupportsAtomicCheckpoints => _connectionOptions.CompatibilityProfile == SqlServerCompatibilityProfile.Default;

    /// <inheritdoc />
    protected override DbConnection CreateCheckpointConnection()
    {
        if (!SupportsAtomicCheckpoints) throw new NotSupportedException("Atomic table-copy checkpoints require a standard SQL Server destination.");
        return CreateTableCopyConnection();
    }

    private SqlConnection CreateTableCopyConnection()
    {
        using var server = new SqlServer { ConnectionOptions = _connectionOptions };
        return server.CreateTableCopyConnection(ConnectionString);
    }

    /// <inheritdoc />
    protected override async Task<string> ResolveCheckpointTableIdentityAsync(DbConnection connection, DbTransaction? transaction, DbaTableCopyDefinition definition, CancellationToken cancellationToken)
    {
        using DbCommand command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandTimeout = CommandTimeout;
        command.CommandText = "SELECT CASE WHEN DB_ID(COALESCE(PARSENAME(@name, 3), DB_NAME())) IS NOT NULL AND OBJECT_ID(@name, N'U') IS NOT NULL THEN CONCAT(DB_ID(COALESCE(PARSENAME(@name, 3), DB_NAME())), ':', OBJECT_ID(@name, N'U')) END";
        command.Parameters.Add(new SqlParameter("@name", SqlDbType.NVarChar, 776) { Value = QuotePath(definition.DestinationName) });
        object? identity = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return identity as string ?? throw new InvalidOperationException($"Checkpoint destination '{definition.DestinationName}' cannot be resolved to a SQL Server table.");
    }

    /// <inheritdoc />
    protected override Task<DataTable> ExecuteBoundedPageCoreAsync(string query, IReadOnlyDictionary<string, object?> parameters, long? maxBytes, CancellationToken cancellationToken)
        => ExecuteSqlServerPageAsync(null, query, parameters, maxBytes, cancellationToken);

    /// <inheritdoc />
    protected override Task<DataTable> ExecuteKeysetPageCoreAsync(DbaTableCopyDefinition definition, string query, IReadOnlyDictionary<string, object?> parameters, long? maxBytes, CancellationToken cancellationToken)
        => ExecuteSqlServerPageAsync(definition, query, parameters, maxBytes, cancellationToken);

    private async Task<DataTable> ExecuteSqlServerPageAsync(DbaTableCopyDefinition? definition, string query, IReadOnlyDictionary<string, object?> parameters, long? maxBytes, CancellationToken cancellationToken)
    {
        using SqlConnection? owned = _readConnection == null ? CreateTableCopyConnection() : null;
        SqlConnection connection = _readConnection ?? owned!;
        if (owned != null) await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        using SqlCommand command = CreateSourceCommand(connection, query);
        if (parameters.Count > 0)
            await AddKeysetParametersAsync(connection, command, definition!, parameters, cancellationToken).ConfigureAwait(false);
        using CancellationTokenRegistration cancellation = cancellationToken.Register(static state => ((SqlCommand)state!).Cancel(), command);
        using SqlDataReader reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken).ConfigureAwait(false);
        try
        {
            return await DbaTableCopyPageReader.ReadAsync(reader, maxBytes, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            cancellationToken.ThrowIfCancellationRequested();
            throw;
        }
        finally
        {
            // A byte-limited page may stop well before TOP(PageSize). SqlDataReader.Close otherwise
            // drains those unused rows (potentially gigabytes) just to obtain RecordsAffected.
            command.Cancel();
        }
    }

    /// <inheritdoc />
    protected override async Task WriteTransactionalPageAsync(DbConnection connection, DbTransaction transaction, DbaTableCopyDefinition definition, DataTable page, DbaTableCopyOptions options, CancellationToken cancellationToken)
    {
        if (_bulkInsertOptions?.AutoCreateTable == true)
            throw new NotSupportedException("Create the destination schema before using atomic table-copy checkpoints.");
        using var server = new SqlServer { ConnectionOptions = _connectionOptions, CommandTimeout = CommandTimeout };
        await server.WriteTableCopyRowsAsync((SqlConnection)connection, (SqlTransaction)transaction, page,
            NormalizeQuotedBulkDestinationTableName(definition.DestinationName), GetEffectiveBulkInsertOptions(options),
            options.BatchSize, options.BulkCopyTimeout, cancellationToken).ConfigureAwait(false);
    }

    private SqlServerBulkInsertOptions? GetEffectiveBulkInsertOptions(DbaTableCopyOptions options)
    {
        if (!options.KeepIdentity && !options.VerifyContent && options.CheckpointId == null) return _bulkInsertOptions;
        SqlBulkCopyOptions flags = _bulkInsertOptions?.BulkCopyOptions ?? SqlBulkCopyOptions.Default;
        if (options.KeepIdentity) flags |= SqlBulkCopyOptions.KeepIdentity;
        if (options.VerifyContent || options.CheckpointId != null) flags |= SqlBulkCopyOptions.KeepNulls | SqlBulkCopyOptions.CheckConstraints;
        return new SqlServerBulkInsertOptions
        {
            BulkCopyOptions = flags,
            AutoCreateTable = _bulkInsertOptions?.AutoCreateTable ?? false,
            ColumnMappings = _bulkInsertOptions?.ColumnMappings,
            NotifyAfter = _bulkInsertOptions?.NotifyAfter,
            RowsCopied = _bulkInsertOptions?.RowsCopied
        };
    }
}
