using System.Data;
using System.Data.Common;
using DBAClientX.DataMovement;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

public sealed partial class SQLiteTableCopyAdapter
{
    /// <inheritdoc />
    public override bool SupportsAtomicCheckpoints => true;

    /// <inheritdoc />
    protected override DbConnection CreateCheckpointConnection() => new SqliteConnection(ResolveSQLiteConnectionString());

    /// <inheritdoc />
    protected override async Task<string> ResolveCheckpointTableIdentityAsync(DbConnection connection, DbTransaction? transaction, DbaTableCopyDefinition definition, CancellationToken cancellationToken)
    {
        string[] segments = DbaIdentifierPath.SplitSegments(definition.DestinationName).Select(DbaIdentifierPath.UnquoteSegment).ToArray();
        if (segments.Length is < 1 or > 2) throw new ArgumentException("SQLite checkpoint destinations require a table name with an optional schema.", nameof(definition));
        string schema = segments.Length == 2 ? segments[0] : "main";
        using DbCommand command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandTimeout = CommandTimeout;
        command.CommandText = $"SELECT name FROM {QuotePath(schema)}.sqlite_master WHERE type='table' AND name=@name COLLATE NOCASE";
        command.Parameters.Add(new SqliteParameter("@name", segments[segments.Length - 1]));
        object? name = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        if (name is not string actualName) throw new InvalidOperationException($"Checkpoint destination '{definition.DestinationName}' does not exist.");
        return DbaIdentifierPath.NormalizeSqliteIdentifier(schema) + ":" + actualName;
    }

    /// <inheritdoc />
    protected override async Task<DataTable> ExecuteBoundedPageCoreAsync(string query, IReadOnlyDictionary<string, object?> parameters, long? maxBytes, CancellationToken cancellationToken)
    {
        using var connection = new SqliteConnection(ResolveSQLiteConnectionString());
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        using SqliteCommand command = connection.CreateCommand();
        command.CommandText = query;
        command.CommandTimeout = CommandTimeout;
        foreach (var parameter in parameters) command.Parameters.AddWithValue(parameter.Key, parameter.Value ?? DBNull.Value);
        using var registration = cancellationToken.Register(() => command.Cancel());
        using SqliteDataReader reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        var fields = new SqliteCopyFieldReader(reader);
        return await DbaTableCopyPageReader.ReadAsync(reader, maxBytes, fields.GetPayloadBytes, fields.ReadValue, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    protected override async Task WriteTransactionalPageAsync(DbConnection connection, DbTransaction transaction, DbaTableCopyDefinition definition, DataTable page, DbaTableCopyOptions options, CancellationToken cancellationToken)
    {
        using var sqlite = new SQLite { CommandTimeout = CommandTimeout };
        await sqlite.WriteBulkRowsAsync((SqliteConnection)connection, (SqliteTransaction)transaction, page,
            NormalizeSQLiteBulkDestinationTableName(definition.DestinationName), options.BatchSize, cancellationToken).ConfigureAwait(false);
    }
}
