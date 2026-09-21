using System.Data;
using DBAClientX.DataMovement;
using Npgsql;

namespace DBAClientX;

public sealed partial class PostgreSqlTableCopyAdapter : IDbaTableCopySchemaPreflightBatchSessionDestination
{
    /// <inheritdoc />
    public async Task<IDbaTableCopySchemaPreflightBatchSession> OpenSchemaPreflightBatchSessionAsync(
        IReadOnlyList<DbaTableCopyDefinition> definitions,
        IReadOnlyList<DataTable?> firstPages,
        DbaTableCopyOptions options,
        CancellationToken cancellationToken)
    {
        ValidateBatchPreflightArguments(definitions, firstPages);
        var connection = new NpgsqlConnection(ConnectionString);
        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            using var postgreSql = new PostgreSql { CommandTimeout = CommandTimeout };
            for (var index = 0; index < definitions.Count; index++)
            {
                DbaTableCopyDefinition definition = definitions[index];
                IReadOnlyList<string> segments = DbaIdentifierPath.SplitSegments(
                    definition.DestinationName,
                    DbaTableCopyProvider.PostgreSql);
                if (segments.Count is < 1 or > 2)
                {
                    throw new ArgumentException(
                        "PostgreSQL table-copy destinations support table or schema.table names.",
                        nameof(definitions));
                }

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
                            $"PostgreSQL destination '{definition.DestinationName}' could not be resolved for coordinated schema preflight.");
                    }
                    schema = reader.GetString(0);
                    table = reader.GetString(1);
                }

                DataTable? firstPage = firstPages[index];
                if (options.ClearDestination)
                {
                    await ValidateRollbackSafeTriggersAsync(
                        connection,
                        definition.DestinationName,
                        cancellationToken).ConfigureAwait(false);
                }
                if (firstPage == null) continue;
                var columns = await postgreSql.GetTableCopyColumnsAsync(connection, schema, table, cancellationToken).ConfigureAwait(false);
                DataTable normalized = DbaPostgreSqlBulkCopyNormalizer.NormalizePage(firstPage, definition.DestinationName);
                using var normalizedToDispose = ReferenceEquals(normalized, firstPage) ? null : normalized;
                string[] projectedColumns = normalized.Columns.Cast<DataColumn>()
                    .Select(static column => column.ColumnName)
                    .ToArray();
                DbaTableCopySchemaValidator.Validate(
                    definition.DestinationName,
                    projectedColumns,
                    columns,
                    static name => name,
                    requirePreservedIdentity: false,
                    keepIdentity: true);
                if (options.ClearDestination)
                    ValidateRollbackSafeGeneratorProjection(definition.DestinationName, projectedColumns, columns);
            }

            var session = new PostgreSqlSchemaPreflightBatchSession(
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

    private sealed class PostgreSqlSchemaPreflightBatchSession : IDbaTableCopySchemaPreflightBatchSession
    {
        private readonly PostgreSqlTableCopyAdapter _owner;
        private readonly NpgsqlConnection _connection;
        private readonly NpgsqlTransaction _transaction;
        private readonly IReadOnlyList<DbaTableCopyDefinition> _definitions;
        private readonly DbaTableCopyOptions _options;
        private bool _disposed;

        internal PostgreSqlSchemaPreflightBatchSession(
            PostgreSqlTableCopyAdapter owner,
            NpgsqlConnection connection,
            NpgsqlTransaction transaction,
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
                    using var clear = new NpgsqlCommand(
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
            if (_disposed) throw new ObjectDisposedException(nameof(PostgreSqlSchemaPreflightBatchSession));
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
                    $"PostgreSQL destination '{definition.DestinationName}' rejected the coordinated projected values or constraints during schema preflight. No destination rows were changed.",
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
