using System.Data;
using DBAClientX.DataMovement;
using DBAClientX.Metadata;
using Oracle.ManagedDataAccess.Client;

namespace DBAClientX;

public sealed partial class OracleTableCopyAdapter : IDbaTableCopySchemaPreflightBatchSessionDestination
{
    /// <inheritdoc />
    public async Task<IDbaTableCopySchemaPreflightBatchSession> OpenSchemaPreflightBatchSessionAsync(
        IReadOnlyList<DbaTableCopyDefinition> definitions,
        IReadOnlyList<DataTable?> firstPages,
        DbaTableCopyOptions options,
        CancellationToken cancellationToken)
    {
        ValidateBatchPreflightArguments(definitions, firstPages);
        var connection = new OracleConnection(ConnectionString);
        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            string? currentOwner = null;
            using var oracle = new Oracle { CommandTimeout = CommandTimeout };
            var destinationColumns = new IReadOnlyList<DbaColumnInfo>?[definitions.Count];
            for (var index = 0; index < definitions.Count; index++)
            {
                DbaTableCopyDefinition definition = definitions[index];
                IReadOnlyList<string> segments = DbaIdentifierPath.SplitSegments(
                    definition.DestinationName,
                    DbaTableCopyProvider.Oracle);
                if (segments.Count is < 1 or > 2)
                {
                    throw new ArgumentException(
                        "Oracle table-copy destinations support table or owner.table names.",
                        nameof(definitions));
                }

                string Normalize(string segment) => DbaIdentifierPath.IsDelimitedSegment(segment)
                    ? DbaIdentifierPath.UnquoteSegment(segment, DbaTableCopyProvider.Oracle)
                    : segment.ToUpperInvariant();
                string owner;
                if (segments.Count == 2)
                {
                    owner = Normalize(segments[0]);
                }
                else
                {
                    if (currentOwner == null)
                    {
                        using var currentSchema = new OracleCommand(
                            "SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM dual",
                            connection)
                        {
                            CommandTimeout = CommandTimeout
                        };
                        currentOwner = Convert.ToString(await currentSchema.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
                        if (string.IsNullOrWhiteSpace(currentOwner))
                            throw new InvalidOperationException("Oracle current schema could not be resolved for coordinated destination preflight.");
                    }
                    owner = currentOwner;
                }

                string table = Normalize(segments[segments.Count - 1]);
                using (var durableTable = new OracleCommand(OracleDurableDestinationTableQuery, connection)
                {
                    BindByName = true,
                    CommandTimeout = CommandTimeout
                })
                {
                    durableTable.Parameters.Add("owner", OracleDbType.Varchar2).Value = owner;
                    durableTable.Parameters.Add("table", OracleDbType.Varchar2).Value = table;
                    if (await durableTable.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) == null)
                    {
                        throw new InvalidOperationException(
                            $"Oracle destination '{definition.DestinationName}' is not a durable table and cannot be used for coordinated schema preflight.");
                    }
                }

                DataTable? firstPage = firstPages[index];
                if (options.ClearDestination)
                {
                    await ValidateRollbackSafeTriggersAsync(
                        connection,
                        owner,
                        table,
                        definition.DestinationName,
                        cancellationToken).ConfigureAwait(false);
                }
                if (firstPage != null || options.ClearDestination)
                {
                    var columns = await oracle.GetTableCopyColumnsAsync(connection, owner, table, cancellationToken).ConfigureAwait(false);
                    destinationColumns[index] = columns;
                    if (firstPage != null)
                    {
                        string[] projectedColumns = firstPage.Columns.Cast<DataColumn>().Select(column =>
                            DbaIdentifierPath.IsDelimitedSegment(column.ColumnName)
                                ? DbaIdentifierPath.UnquoteSegment(column.ColumnName, DbaTableCopyProvider.Oracle)
                                : column.ColumnName.ToUpperInvariant()).ToArray();
                        DbaTableCopySchemaValidator.Validate(
                            definition.DestinationName,
                            projectedColumns,
                            columns,
                            static name => name,
                            requirePreservedIdentity: false,
                            keepIdentity: true);
                        ValidateProjectedIdentityColumns(definition.DestinationName, projectedColumns, columns);
                        if (options.ClearDestination)
                        {
                            ValidateRollbackSafeGeneratorProjection(definition.DestinationName, projectedColumns, columns);
                            ValidateRollbackSafeGeneratorValues(definition.DestinationName, firstPage, columns);
                        }
                    }
                }
            }

            var session = new OracleSchemaPreflightBatchSession(
                this,
                connection,
                connection.BeginTransaction(IsolationLevel.ReadCommitted),
                definitions,
                options,
                destinationColumns);
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

    private sealed class OracleSchemaPreflightBatchSession : IDbaTableCopySchemaPreflightBatchSession
    {
        private readonly OracleTableCopyAdapter _owner;
        private readonly OracleConnection _connection;
        private readonly OracleTransaction _transaction;
        private readonly IReadOnlyList<DbaTableCopyDefinition> _definitions;
        private readonly DbaTableCopyOptions _options;
        private readonly IReadOnlyList<DbaColumnInfo>?[] _destinationColumns;
        private bool _disposed;

        internal OracleSchemaPreflightBatchSession(
            OracleTableCopyAdapter owner,
            OracleConnection connection,
            OracleTransaction transaction,
            IReadOnlyList<DbaTableCopyDefinition> definitions,
            DbaTableCopyOptions options,
            IReadOnlyList<DbaColumnInfo>?[] destinationColumns)
        {
            _owner = owner;
            _connection = connection;
            _transaction = transaction;
            _definitions = definitions;
            _options = options;
            _destinationColumns = destinationColumns;
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
                            columns);
                    }
                }
                for (var index = _definitions.Count - 1; index >= 0; index--)
                {
                    using var clear = new OracleCommand(
                        $"DELETE FROM {_owner.QuotePath(_definitions[index].DestinationName)}",
                        _connection)
                    {
                        Transaction = _transaction,
                        CommandTimeout = _owner.CommandTimeout
                    };
                    await clear.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
            }

        }

        public async Task ValidatePageAsync(int definitionIndex, DataTable page, CancellationToken cancellationToken)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(OracleSchemaPreflightBatchSession));
            if ((uint)definitionIndex >= (uint)_definitions.Count) throw new ArgumentOutOfRangeException(nameof(definitionIndex));
            if (page.Rows.Count == 0) return;
            DbaTableCopyDefinition definition = _definitions[definitionIndex];
            if (_options.ClearDestination && _destinationColumns[definitionIndex] is { } columns)
            {
                ValidateRollbackSafeGeneratorValues(definition.DestinationName, page, columns);
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
                    $"Oracle destination '{definition.DestinationName}' rejected the coordinated projected values or constraints during schema preflight. No destination rows were changed.",
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
