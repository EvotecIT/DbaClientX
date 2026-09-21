using System.Data;
using DBAClientX.DataMovement;
using DBAClientX.Metadata;
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
            bool noAutoValueOnZero = options.ClearDestination &&
                await ResolveNoAutoValueOnZeroAsync(connection, cancellationToken).ConfigureAwait(false);
            var destinationColumns = new IReadOnlyList<DbaColumnInfo>?[definitions.Count];
            var nextAutoIncrementValues = new decimal?[definitions.Count];
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
                if (firstPage != null || options.ClearDestination)
                {
                    var columns = await mySql.GetTableCopyColumnsAsync(connection, database, table, cancellationToken).ConfigureAwait(false);
                    destinationColumns[index] = columns;
                    if (options.ClearDestination && columns.Any(static column => column.IsIdentity == true))
                    {
                        nextAutoIncrementValues[index] = await ResolveNextAutoIncrementAsync(
                            connection,
                            database,
                            table,
                            cancellationToken).ConfigureAwait(false);
                    }
                    if (firstPage != null)
                    {
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
                        {
                            ValidateRollbackSafeGeneratorProjection(definition.DestinationName, projectedColumns, columns);
                            ValidateRollbackSafeGeneratorValues(
                                definition.DestinationName,
                                firstPage,
                                columns,
                                noAutoValueOnZero,
                                nextAutoIncrementValues[index]);
                        }
                    }
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
                options,
                destinationColumns,
                noAutoValueOnZero,
                nextAutoIncrementValues);
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
        private readonly IReadOnlyList<DbaColumnInfo>?[] _destinationColumns;
        private readonly bool _noAutoValueOnZero;
        private readonly decimal?[] _nextAutoIncrementValues;
        private bool _disposed;

        internal MySqlSchemaPreflightBatchSession(
            MySqlTableCopyAdapter owner,
            MySqlConnection connection,
            MySqlTransaction transaction,
            IReadOnlyList<DbaTableCopyDefinition> definitions,
            DbaTableCopyOptions options,
            IReadOnlyList<DbaColumnInfo>?[] destinationColumns,
            bool noAutoValueOnZero,
            decimal?[] nextAutoIncrementValues)
        {
            _owner = owner;
            _connection = connection;
            _transaction = transaction;
            _definitions = definitions;
            _options = options;
            _destinationColumns = destinationColumns;
            _noAutoValueOnZero = noAutoValueOnZero;
            _nextAutoIncrementValues = nextAutoIncrementValues;
        }

        internal async Task InitializeAsync(IReadOnlyList<DataTable?> firstPages, CancellationToken cancellationToken)
        {
            if (_options.ClearDestination)
            {
                for (var index = 0; index < _definitions.Count; index++)
                {
                    DataTable? firstPage = firstPages[index];
                    IReadOnlyList<DbaColumnInfo>? columns = _destinationColumns[index];
                    if (firstPage != null && columns != null)
                    {
                        ValidateRollbackSafeGeneratorValues(
                            _definitions[index].DestinationName,
                            firstPage,
                            columns,
                            _noAutoValueOnZero,
                            _nextAutoIncrementValues[index]);
                    }
                }
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
            if (_options.ClearDestination && _destinationColumns[definitionIndex] is { } columns)
            {
                ValidateRollbackSafeGeneratorValues(
                    definition.DestinationName,
                    page,
                    columns,
                    _noAutoValueOnZero,
                    _nextAutoIncrementValues[definitionIndex]);
            }
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
