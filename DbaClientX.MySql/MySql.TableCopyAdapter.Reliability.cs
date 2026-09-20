using System.Data;
using System.Data.Common;
using DBAClientX.DataMovement;
using MySqlConnector;

namespace DBAClientX;

public sealed partial class MySqlTableCopyAdapter : IDbaTableCopySchemaPreflightDestination
{
    /// <inheritdoc />
    public override bool SupportsAtomicCheckpoints => true;

    /// <inheritdoc />
    protected override DbConnection CreateCheckpointConnection()
        => new MySqlConnection(ConnectionString);

    /// <inheritdoc />
    protected override void ValidateCheckpointStorage(DbConnection connection)
        => ValidateCheckpointDatabase(((MySqlConnection)connection).Database);

    internal static void ValidateCheckpointDatabase(string? database)
    {
        if (!string.IsNullOrWhiteSpace(database)) return;
        throw new InvalidOperationException(
            "Atomic MySQL checkpoints require a selected database in the connection string. Database-qualified destination names do not select checkpoint storage.");
    }

    /// <inheritdoc />
    protected override async Task ValidateCheckpointSchemaAsync(
        DbConnection connection,
        CancellationToken cancellationToken)
    {
        await using var command = new MySqlCommand(
            "SELECT ENGINE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'DbaClientX_TableCopyCheckpoints'",
            (MySqlConnection)connection)
        {
            CommandTimeout = CommandTimeout
        };
        var engine = Convert.ToString(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
        if (!string.Equals(engine, "InnoDB", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                "Atomic MySQL checkpoints require the DbaClientX checkpoint table to use the InnoDB storage engine.");
        }
    }

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
            "SELECT CONCAT(TABLE_SCHEMA, ':', TABLE_NAME), ENGINE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = @database AND TABLE_NAME = @table",
            (MySqlConnection)connection,
            (MySqlTransaction?)transaction)
        {
            CommandTimeout = CommandTimeout
        };
        command.Parameters.AddWithValue("@database", database);
        command.Parameters.AddWithValue("@table", segments[segments.Length - 1]);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            throw new InvalidOperationException(
                $"Checkpoint destination '{definition.DestinationName}' cannot be resolved to a MySQL table.");
        }

        var identity = reader.GetString(0);
        var engine = reader.IsDBNull(1) ? null : reader.GetString(1);
        if (!string.Equals(engine, "InnoDB", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"Atomic MySQL checkpoints require destination table '{definition.DestinationName}' to use the InnoDB storage engine; found '{engine ?? "unknown"}'.");
        }

        return identity;
    }

    /// <inheritdoc />
    public async Task ValidateSchemaAsync(
        DbaTableCopyDefinition definition,
        DataTable page,
        DbaTableCopyOptions options,
        CancellationToken cancellationToken)
    {
        string[] segments = DbaIdentifierPath.SplitSegments(
                definition.DestinationName,
                DbaTableCopyProvider.MySql)
            .Select(segment => DbaIdentifierPath.UnquoteSegment(segment, DbaTableCopyProvider.MySql))
            .ToArray();
        if (segments.Length is < 1 or > 2)
        {
            throw new ArgumentException(
                "MySQL table-copy destinations support table or database.table names.",
                nameof(definition));
        }

        await using var connection = new MySqlConnection(ConnectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        string database = segments.Length == 2 ? segments[0] : connection.Database;
        if (string.IsNullOrWhiteSpace(database))
        {
            throw new InvalidOperationException(
                $"MySQL destination '{definition.DestinationName}' requires a selected database or a database-qualified table name for schema validation.");
        }

        using var mySql = new MySql { CommandTimeout = CommandTimeout };
        var columns = await mySql.GetTableCopyColumnsAsync(
            connection,
            database,
            segments[segments.Length - 1],
            cancellationToken).ConfigureAwait(false);
        DbaTableCopySchemaValidator.Validate(
            definition.DestinationName,
            page.Columns.Cast<DataColumn>().Select(static column => column.ColumnName).ToArray(),
            columns,
            static name => DbaIdentifierPath.UnquoteSegment(name, DbaTableCopyProvider.MySql).ToUpperInvariant(),
            requirePreservedIdentity: false,
            keepIdentity: true);
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
