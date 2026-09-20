using System.Data;
using System.Data.Common;
using DBAClientX.DataMovement;
using DBAClientX.Metadata;
using Npgsql;

namespace DBAClientX;

public sealed partial class PostgreSqlTableCopyAdapter : IDbaTableCopySchemaPreflightDestination
{
    internal const string PostgreSqlCheckpointDestinationIdentityQuery = @"SELECT current_database() || ':' || cls.oid::text
FROM pg_catalog.pg_class AS cls
WHERE cls.oid = to_regclass(@name)
  AND cls.relkind IN ('r', 'p')
  AND cls.relpersistence = 'p'";

    internal const string PostgreSqlSchemaPreflightDestinationQuery = @"
SELECT ns.nspname, cls.relname
FROM pg_catalog.pg_class AS cls
JOIN pg_catalog.pg_namespace AS ns ON ns.oid = cls.relnamespace
WHERE cls.oid = to_regclass(@name)
  AND cls.relkind IN ('r', 'p')
  AND cls.relpersistence = 'p'";

    internal const string PostgreSqlCheckpointStorageDurabilityQuery = @"SELECT cls.relpersistence
FROM pg_catalog.pg_class AS cls
WHERE cls.oid = to_regclass(@name)
  AND cls.relkind IN ('r', 'p')";

    /// <inheritdoc />
    public override bool SupportsAtomicCheckpoints => true;

    /// <inheritdoc />
    protected override DbConnection CreateCheckpointConnection() => new NpgsqlConnection(ConnectionString);

    /// <inheritdoc />
    protected override async Task ValidateCheckpointSchemaAsync(
        DbConnection connection,
        CancellationToken cancellationToken)
    {
        using var command = new NpgsqlCommand(
            PostgreSqlCheckpointStorageDurabilityQuery,
            (NpgsqlConnection)connection)
        {
            CommandTimeout = CommandTimeout
        };
        command.Parameters.AddWithValue("@name", "\"DbaClientX_TableCopyCheckpoints\"");
        string? persistence = Convert.ToString(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
        ValidateCheckpointStorageDurability(persistence);
    }

    internal static void ValidateCheckpointStorageDurability(string? persistence)
    {
        if (!string.Equals(persistence, "p", StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                "Atomic PostgreSQL checkpoints require DbaClientX_TableCopyCheckpoints to be a permanent logged table in the active search path.");
        }
    }

    /// <inheritdoc />
    protected override async Task<string> ResolveCheckpointTableIdentityAsync(
        DbConnection connection,
        DbTransaction? transaction,
        DbaTableCopyDefinition definition,
        CancellationToken cancellationToken)
    {
        var segments = DbaIdentifierPath.SplitSegments(definition.DestinationName, DbaTableCopyProvider.PostgreSql);
        if (segments.Count is < 1 or > 2)
        {
            throw new ArgumentException(
                "PostgreSQL checkpoint destinations require a table name with an optional schema.",
                nameof(definition));
        }

        using var command = new NpgsqlCommand(
            PostgreSqlCheckpointDestinationIdentityQuery,
            (NpgsqlConnection)connection,
            (NpgsqlTransaction?)transaction)
        {
            CommandTimeout = CommandTimeout
        };
        command.Parameters.AddWithValue("@name", QuotePath(definition.DestinationName));
        var identity = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return identity as string ?? throw new InvalidOperationException(
            $"Checkpoint destination '{definition.DestinationName}' cannot be resolved to a PostgreSQL table.");
    }

    /// <inheritdoc />
    public async Task ValidateSchemaAsync(
        DbaTableCopyDefinition definition,
        DataTable page,
        DbaTableCopyOptions options,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<string> rawSegments = DbaIdentifierPath.SplitSegments(
            definition.DestinationName,
            DbaTableCopyProvider.PostgreSql);
        if (rawSegments.Count is < 1 or > 2)
        {
            throw new ArgumentException(
                "PostgreSQL table-copy destinations support table or schema.table names.",
                nameof(definition));
        }

        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        using var resolve = new NpgsqlCommand(PostgreSqlSchemaPreflightDestinationQuery, connection)
        {
            CommandTimeout = CommandTimeout
        };
        resolve.Parameters.AddWithValue("@name", QuotePath(definition.DestinationName));
        string schema;
        string table;
        using (NpgsqlDataReader reader = await resolve.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                throw new InvalidOperationException(
                    $"PostgreSQL destination '{definition.DestinationName}' could not be resolved for schema preflight.");
            }

            schema = reader.GetString(0);
            table = reader.GetString(1);
        }

        using var postgreSql = new PostgreSql { CommandTimeout = CommandTimeout };
        var columns = await postgreSql.GetTableCopyColumnsAsync(
            connection,
            schema,
            table,
            cancellationToken).ConfigureAwait(false);
        DataTable normalizedPage = DbaPostgreSqlBulkCopyNormalizer.NormalizePage(page, definition.DestinationName);
        using var normalizedPageToDispose = ReferenceEquals(normalizedPage, page) ? null : normalizedPage;
        DbaTableCopySchemaValidator.Validate(
            definition.DestinationName,
            normalizedPage.Columns.Cast<DataColumn>().Select(static column => column.ColumnName).ToArray(),
            columns,
            static name => name,
            requirePreservedIdentity: false,
            keepIdentity: true);
        await ValidateDestinationWriteAsync(
            connection,
            definition,
            normalizedPage,
            options,
            cancellationToken).ConfigureAwait(false);
    }

    private async Task ValidateDestinationWriteAsync(
        NpgsqlConnection connection,
        DbaTableCopyDefinition definition,
        DataTable normalizedPage,
        DbaTableCopyOptions options,
        CancellationToken cancellationToken)
    {
        if (normalizedPage.Rows.Count == 0) return;

        cancellationToken.ThrowIfCancellationRequested();
        using NpgsqlTransaction transaction = connection.BeginTransaction();
        try
        {
            if (options.ClearDestination)
            {
                using var clear = new NpgsqlCommand(
                    $"DELETE FROM {QuotePath(definition.DestinationName)}",
                    connection,
                    transaction)
                {
                    CommandTimeout = CommandTimeout
                };
                await clear.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await WriteTransactionalPageAsync(
                connection,
                transaction,
                definition,
                normalizedPage,
                options,
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            throw new InvalidOperationException(
                $"PostgreSQL destination '{definition.DestinationName}' rejected the projected CLR types, values, or constraints during schema preflight. No destination rows were changed.",
                exception);
        }
        finally
        {
            transaction.Rollback();
        }
    }

    /// <inheritdoc />
    protected override async Task WriteTransactionalPageAsync(
        DbConnection connection,
        DbTransaction transaction,
        DbaTableCopyDefinition definition,
        DataTable page,
        DbaTableCopyOptions options,
        CancellationToken cancellationToken)
    {
        var bulkPage = DbaPostgreSqlBulkCopyNormalizer.NormalizePage(page, definition.DestinationName);
        using var bulkPageToDispose = ReferenceEquals(bulkPage, page) ? null : bulkPage;
        using var postgreSql = new PostgreSql { CommandTimeout = CommandTimeout };
        await postgreSql.WriteTableCopyRowsAsync(
            (NpgsqlConnection)connection,
            (NpgsqlTransaction)transaction,
            bulkPage,
            DbaPostgreSqlBulkCopyNormalizer.NormalizeDestinationTableName(definition.DestinationName),
            options.BulkCopyTimeout,
            cancellationToken).ConfigureAwait(false);
    }
}
