using System.Data;
using System.Data.Common;
using DBAClientX.DataMovement;
using Npgsql;

namespace DBAClientX;

public sealed partial class PostgreSqlTableCopyAdapter
{
    /// <inheritdoc />
    public override bool SupportsAtomicCheckpoints => true;

    /// <inheritdoc />
    protected override DbConnection CreateCheckpointConnection() => new NpgsqlConnection(ConnectionString);

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
            "SELECT current_database() || ':' || to_regclass(@name)::oid::text",
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
