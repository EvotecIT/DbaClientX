using System.Data;
using DBAClientX.DataMovement;
using MySqlConnector;

namespace DBAClientX;

public sealed partial class MySqlTableCopyAdapter : IDbaTableCopySchemaPreflightBatchSessionDestination
{
    /// <inheritdoc />
    public async Task<IDbaTableCopySchemaPreflightBatchSession> OpenSchemaPreflightBatchSessionAsync(
        IReadOnlyList<DbaTableCopyDefinition> definitions,
        IReadOnlyList<DataTable?> firstPages,
        DbaTableCopyOptions options,
        CancellationToken cancellationToken)
    {
        ValidateBatchPreflightArguments(definitions, firstPages);
        var connection = new MySqlConnection(ConnectionString);
        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            using var mySql = new MySql { CommandTimeout = CommandTimeout };
            for (var index = 0; index < definitions.Count; index++)
            {
                DbaTableCopyDefinition definition = definitions[index];
                string[] segments = DbaIdentifierPath.SplitSegments(
                        definition.DestinationName,
                        DbaTableCopyProvider.MySql)
                    .Select(segment => DbaIdentifierPath.UnquoteSegment(segment, DbaTableCopyProvider.MySql))
                    .ToArray();
                if (segments.Length is < 1 or > 2)
                {
                    throw new ArgumentException(
                        "MySQL table-copy destinations support table or database.table names.",
                        nameof(definitions));
                }

                string database = segments.Length == 2 ? segments[0] : connection.Database;
                if (string.IsNullOrWhiteSpace(database))
                {
                    throw new InvalidOperationException(
                        $"MySQL destination '{definition.DestinationName}' requires a selected database or a database-qualified table name for coordinated schema validation.");
                }

                string table = segments[segments.Length - 1];
                if (options.ClearDestination)
                {
                    await ValidateRollbackSafeTriggersAsync(
                        connection,
                        database,
                        table,
                        definition.DestinationName,
                        cancellationToken).ConfigureAwait(false);
                }
                DataTable? firstPage = firstPages[index];
                if (firstPage != null)
                {
                    var columns = await mySql.GetTableCopyColumnsAsync(connection, database, table, cancellationToken).ConfigureAwait(false);
                    string[] projectedColumns = firstPage.Columns.Cast<DataColumn>()
                        .Select(static column => column.ColumnName)
                        .ToArray();
                    DbaTableCopySchemaValidator.Validate(
                        definition.DestinationName,
                        projectedColumns,
                        columns,
                        static name => DbaIdentifierPath.UnquoteSegment(name, DbaTableCopyProvider.MySql).ToUpperInvariant(),
                        requirePreservedIdentity: false,
                        keepIdentity: true);
                    if (options.ClearDestination)
                        ValidateRollbackSafeGeneratorProjection(definition.DestinationName, projectedColumns, columns);
                }

                await EnsureTransactionalPreflightDestinationAsync(
                    connection,
                    definition.DestinationName,
                    database,
                    table,
                    cancellationToken).ConfigureAwait(false);
            }

            var session = new MySqlSchemaPreflightBatchSession(
                this,
                connection,
                connection.BeginTransaction(),
                definitions,
                options);
            try
            {
                await session.InitializeAsync(firstPages, cancellationToken).ConfigureAwait(false);
                return session;
            }
            catch
            {
                await session.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }
        catch
        {
            connection.Dispose();
            throw;
        }
    }

    private static void ValidateBatchPreflightArguments(
        IReadOnlyList<DbaTableCopyDefinition> definitions,
        IReadOnlyList<DataTable?> firstPages)
    {
        if (definitions == null) throw new ArgumentNullException(nameof(definitions));
        if (firstPages == null) throw new ArgumentNullException(nameof(firstPages));
        if (definitions.Count == 0 || definitions.Count != firstPages.Count)
            throw new ArgumentException("Coordinated schema preflight requires one first-page slot per definition.", nameof(firstPages));
    }

    private sealed class MySqlSchemaPreflightBatchSession : IDbaTableCopySchemaPreflightBatchSession
    {
        private readonly MySqlTableCopyAdapter _owner;
        private readonly MySqlConnection _connection;
        private readonly MySqlTransaction _transaction;
        private readonly IReadOnlyList<DbaTableCopyDefinition> _definitions;
        private readonly DbaTableCopyOptions _options;
        private bool _disposed;

        internal MySqlSchemaPreflightBatchSession(
            MySqlTableCopyAdapter owner,
            MySqlConnection connection,
            MySqlTransaction transaction,
            IReadOnlyList<DbaTableCopyDefinition> definitions,
            DbaTableCopyOptions options)
        {
            _owner = owner;
            _connection = connection;
            _transaction = transaction;
            _definitions = definitions;
            _options = options;
        }

        internal async Task InitializeAsync(IReadOnlyList<DataTable?> firstPages, CancellationToken cancellationToken)
        {
            if (_options.ClearDestination)
            {
                for (var index = _definitions.Count - 1; index >= 0; index--)
                {
                    await using var clear = new MySqlCommand(
                        $"DELETE FROM {_owner.QuotePath(_definitions[index].DestinationName)}",
                        _connection,
                        _transaction)
                    {
                        CommandTimeout = _owner.CommandTimeout
                    };
                    await clear.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
            }

        }

        public async Task ValidatePageAsync(int definitionIndex, DataTable page, CancellationToken cancellationToken)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(MySqlSchemaPreflightBatchSession));
            if ((uint)definitionIndex >= (uint)_definitions.Count) throw new ArgumentOutOfRangeException(nameof(definitionIndex));
            if (page.Rows.Count == 0) return;
            DbaTableCopyDefinition definition = _definitions[definitionIndex];
            try
            {
                await _owner.WriteTransactionalPageAsync(
                    _connection,
                    _transaction,
                    definition,
                    page,
                    _options,
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                throw new InvalidOperationException(
                    $"MySQL destination '{definition.DestinationName}' rejected the coordinated projected values or constraints during schema preflight. No destination rows were changed.",
                    exception);
            }
        }

        public ValueTask DisposeAsync()
        {
            if (_disposed) return default;
            _disposed = true;
            try { _transaction.Rollback(); }
            finally
            {
                _transaction.Dispose();
                _connection.Dispose();
            }
            return default;
        }
    }
}
