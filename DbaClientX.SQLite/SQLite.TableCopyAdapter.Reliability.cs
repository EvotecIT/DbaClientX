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
        return await DbaTableCopyPageReader.ReadAsync(reader, maxBytes, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    protected override async Task WriteTransactionalPageAsync(DbConnection connection, DbTransaction transaction, DbaTableCopyDefinition definition, DataTable page, DbaTableCopyOptions options, CancellationToken cancellationToken)
    {
        using var sqlite = new SQLite { CommandTimeout = CommandTimeout };
        await sqlite.WriteBulkRowsAsync((SqliteConnection)connection, (SqliteTransaction)transaction, page,
            NormalizeSQLiteBulkDestinationTableName(definition.DestinationName), options.BatchSize, cancellationToken).ConfigureAwait(false);
    }
}
