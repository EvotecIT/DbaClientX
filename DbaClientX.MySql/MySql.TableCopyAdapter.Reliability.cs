using System.Data;
using System.Data.Common;
using DBAClientX.DataMovement;
using MySqlConnector;

namespace DBAClientX;

public sealed partial class MySqlTableCopyAdapter
{
    /// <inheritdoc />
    public override bool SupportsAtomicCheckpoints => true;

    /// <inheritdoc />
    protected override DbConnection CreateCheckpointConnection()
        => new MySqlConnection(ConnectionString);

    /// <inheritdoc />
    protected override async Task<string> ResolveCheckpointTableIdentityAsync(
        DbConnection connection,
        DbTransaction? transaction,
        DbaTableCopyDefinition definition,
        CancellationToken cancellationToken)
    {
        var segments = DbaIdentifierPath.SplitSegments(definition.DestinationName, DbaTableCopyProvider.MySql)
            .Select(segment => DbaIdentifierPath.UnquoteSegment(segment, DbaTableCopyProvider.MySql))
            .ToArray();
        if (segments.Length is < 1 or > 2)
        {
            throw new ArgumentException(
                "MySQL checkpoint destinations require a table name with an optional database.",
                nameof(definition));
        }

        var database = segments.Length == 2 ? segments[0] : ((MySqlConnection)connection).Database;
        await using var command = new MySqlCommand(
            "SELECT CONCAT(TABLE_SCHEMA, ':', TABLE_NAME) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = @database AND TABLE_NAME = @table",
            (MySqlConnection)connection,
            (MySqlTransaction?)transaction)
        {
            CommandTimeout = CommandTimeout
        };
        command.Parameters.AddWithValue("@database", database);
        command.Parameters.AddWithValue("@table", segments[segments.Length - 1]);
        var identity = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return identity as string ?? throw new InvalidOperationException(
            $"Checkpoint destination '{definition.DestinationName}' cannot be resolved to a MySQL table.");
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
        ValidatePage(definition, page);
        using var mySql = new MySql { CommandTimeout = CommandTimeout };
        await mySql.WriteTableCopyRowsAsync(
            (MySqlConnection)connection,
            (MySqlTransaction)transaction,
            page,
            NormalizeQuotedBulkDestinationTableName(definition.DestinationName),
            options.BatchSize,
            options.BulkCopyTimeout,
            cancellationToken).ConfigureAwait(false);
    }
}
