using System.Data;
using DBAClientX.DataMovement;
using DBAClientX.Metadata;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public sealed partial class SqlServerTableCopyAdapter :
    IDbaTableCopySchemaPreflightSessionDestination,
    IDbaTableCopySchemaPreflightBatchSessionDestination
{
    internal const string SqlServerRollbackUnsafeTriggerQuery = @"WITH cascade_targets AS (
    SELECT OBJECT_ID(@name, N'U') AS object_id, 0 AS depth
    UNION ALL
    SELECT foreign_key.parent_object_id, cascade_targets.depth + 1
    FROM sys.foreign_keys AS foreign_key
    INNER JOIN cascade_targets ON cascade_targets.object_id = foreign_key.referenced_object_id
    WHERE foreign_key.is_disabled = 0
      AND foreign_key.delete_referential_action = 1
)
SELECT TOP (1) 1
FROM sys.triggers AS trigger_info
INNER JOIN cascade_targets ON cascade_targets.object_id = trigger_info.parent_id
WHERE trigger_info.is_disabled = 0
  AND (OBJECTPROPERTY(trigger_info.object_id, 'ExecIsDeleteTrigger') = 1
       OR (cascade_targets.depth = 0
           AND OBJECTPROPERTY(trigger_info.object_id, 'ExecIsInsertTrigger') = 1))
OPTION (MAXRECURSION 32767)";

    /// <inheritdoc />
    public async Task<IDbaTableCopySchemaPreflightSession> OpenSchemaPreflightSessionAsync(
        DbaTableCopyDefinition definition,
        DataTable firstPage,
        DbaTableCopyOptions options,
        CancellationToken cancellationToken)
    {
        var connection = CreateTableCopyConnection();
        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            IReadOnlyList<DbaColumnInfo> columns = await ValidateSchemaAsync(
                connection, definition, firstPage, options, cancellationToken).ConfigureAwait(false);
            await ValidateRollbackSafetyAsync(connection, definition, firstPage, columns, options, cancellationToken).ConfigureAwait(false);
            var session = new SqlServerSchemaPreflightSession(
                this, connection, connection.BeginTransaction(), definition, options);
            try
            {
                await session.InitializeAsync(firstPage, cancellationToken).ConfigureAwait(false);
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

    /// <inheritdoc />
    public async Task<IDbaTableCopySchemaPreflightBatchSession> OpenSchemaPreflightBatchSessionAsync(
        IReadOnlyList<DbaTableCopyDefinition> definitions,
        IReadOnlyList<DataTable?> firstPages,
        DbaTableCopyOptions options,
        CancellationToken cancellationToken)
    {
        ValidateBatchArguments(definitions, firstPages);
        var connection = CreateTableCopyConnection();
        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            SelectBatchPreflightDatabase(connection, definitions);
            for (var index = 0; index < definitions.Count; index++)
            {
                await ValidateRollbackSafeTriggersAsync(
                    connection, definitions[index].DestinationName, cancellationToken).ConfigureAwait(false);
                if (firstPages[index] is not DataTable page) continue;
                IReadOnlyList<DbaColumnInfo> columns = await ValidateSchemaAsync(
                    connection, definitions[index], page, options, cancellationToken).ConfigureAwait(false);
                ValidateRollbackSafeGeneratorProjection(
                    definitions[index].DestinationName,
                    GetProjectedDestinationColumns(page),
                    columns,
                    HasKeepIdentity(options));
            }

            var session = new SqlServerSchemaPreflightBatchSession(
                this, connection, connection.BeginTransaction(), definitions, options);
            try
            {
                await session.InitializeAsync(cancellationToken).ConfigureAwait(false);
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

    private async Task ValidateRollbackSafetyAsync(
        SqlConnection connection,
        DbaTableCopyDefinition definition,
        DataTable page,
        IReadOnlyList<DbaColumnInfo> columns,
        DbaTableCopyOptions options,
        CancellationToken cancellationToken)
    {
        ValidateRollbackSafeGeneratorProjection(
            definition.DestinationName,
            GetProjectedDestinationColumns(page),
            columns,
            HasKeepIdentity(options));
        await ValidateRollbackSafeTriggersAsync(
            connection, definition.DestinationName, cancellationToken).ConfigureAwait(false);
    }

    private bool HasKeepIdentity(DbaTableCopyOptions options)
        => ((GetEffectiveBulkInsertOptions(options)?.BulkCopyOptions ?? SqlBulkCopyOptions.Default) & SqlBulkCopyOptions.KeepIdentity) != 0;

    private IReadOnlyCollection<string> GetProjectedDestinationColumns(DataTable page)
        => page.Columns.Cast<DataColumn>().Select(column =>
            _bulkInsertOptions?.ColumnMappings?.TryGetValue(column.ColumnName, out string? mapped) == true
                ? mapped
                : column.ColumnName).ToArray();

    internal static void ValidateRollbackSafeGeneratorProjection(
        string tableName,
        IReadOnlyCollection<string> projectedColumns,
        IReadOnlyList<DbaColumnInfo> destinationColumns,
        bool keepIdentity)
    {
        var projected = new HashSet<string>(projectedColumns, StringComparer.OrdinalIgnoreCase);
        DbaColumnInfo? identityColumn = destinationColumns.FirstOrDefault(static column => column.IsIdentity == true);
        if (identityColumn != null && !keepIdentity)
        {
            throw new InvalidOperationException(
                $"SQL Server destination '{tableName}' uses an identity generator. ClearDestination all-page preflight requires KeepIdentity and explicit identity values so validation does not consume nontransactional identity values.");
        }

        DbaColumnInfo? omittedIdentity = destinationColumns.FirstOrDefault(column =>
            column.IsIdentity == true && !projected.Contains(column.Name));
        if (omittedIdentity != null)
        {
            throw new InvalidOperationException(
                $"SQL Server destination '{tableName}' omits identity column '{omittedIdentity.Name}'. ClearDestination cannot safely preflight this projection because identity advances are not rolled back. Project an explicit value for the column with KeepIdentity, or copy without ClearDestination.");
        }

        DbaColumnInfo? sequenceColumn = destinationColumns.FirstOrDefault(column =>
            !projected.Contains(column.Name) &&
            column.DefaultExpression?.IndexOf("NEXT VALUE FOR", StringComparison.OrdinalIgnoreCase) >= 0);
        if (sequenceColumn != null)
        {
            throw new InvalidOperationException(
                $"SQL Server destination '{tableName}' omits sequence-backed column '{sequenceColumn.Name}'. ClearDestination cannot preflight it without consuming nontransactional sequence values.");
        }
    }

    private async Task ValidateRollbackSafeTriggersAsync(
        SqlConnection connection,
        string destinationName,
        CancellationToken cancellationToken)
    {
        using var trigger = new SqlCommand(SqlServerRollbackUnsafeTriggerQuery, connection)
        {
            CommandTimeout = CommandTimeout
        };
        trigger.Parameters.Add(new SqlParameter("@name", SqlDbType.NVarChar, 776) { Value = QuotePath(destinationName) });
        if (await trigger.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) != null)
        {
            throw new InvalidOperationException(
                $"SQL Server destination '{destinationName}' has an enabled INSERT or DELETE trigger. ClearDestination cannot safely preflight trigger side effects.");
        }
    }

    private static void ValidateBatchArguments(
        IReadOnlyList<DbaTableCopyDefinition> definitions,
        IReadOnlyList<DataTable?> firstPages)
    {
        if (definitions == null) throw new ArgumentNullException(nameof(definitions));
        if (firstPages == null) throw new ArgumentNullException(nameof(firstPages));
        if (definitions.Count == 0 || definitions.Count != firstPages.Count)
            throw new ArgumentException("Coordinated schema preflight requires one first-page slot per definition.", nameof(firstPages));
    }

    private static void SelectBatchPreflightDatabase(
        SqlConnection connection,
        IReadOnlyList<DbaTableCopyDefinition> definitions)
    {
        string defaultDatabase = connection.Database;
        string[] databases = definitions
            .Select(definition => DbaIdentifierPath.SplitSegments(definition.DestinationName)
                .Select(DbaIdentifierPath.UnquoteSegment)
                .ToArray())
            .Select(parts => parts.Length == 3 ? parts[0] : defaultDatabase)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
        if (databases.Length != 1)
        {
            throw new InvalidOperationException(
                "SQL Server coordinated schema preflight requires every destination to use the same database so all validation can run in one rollback-only transaction.");
        }

        if (!string.Equals(connection.Database, databases[0], StringComparison.OrdinalIgnoreCase))
            connection.ChangeDatabase(databases[0]);
    }

    private sealed class SqlServerSchemaPreflightSession : IDbaTableCopySchemaPreflightSession
    {
        private readonly SqlServerTableCopyAdapter _owner;
        private readonly SqlConnection _connection;
        private readonly SqlTransaction _transaction;
        private readonly DbaTableCopyDefinition _definition;
        private readonly DbaTableCopyOptions _options;
        private bool _disposed;

        internal SqlServerSchemaPreflightSession(
            SqlServerTableCopyAdapter owner,
            SqlConnection connection,
            SqlTransaction transaction,
            DbaTableCopyDefinition definition,
            DbaTableCopyOptions options)
        {
            _owner = owner;
            _connection = connection;
            _transaction = transaction;
            _definition = definition;
            _options = options;
        }

        internal async Task InitializeAsync(DataTable firstPage, CancellationToken cancellationToken)
        {
            if (_options.ClearDestination)
            {
                using var clear = new SqlCommand(
                    $"DELETE FROM {_owner.QuotePath(_definition.DestinationName)}",
                    _connection,
                    _transaction)
                {
                    CommandTimeout = _owner.CommandTimeout
                };
                await clear.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
            await ValidatePageAsync(firstPage, cancellationToken).ConfigureAwait(false);
        }

        public async Task ValidatePageAsync(DataTable page, CancellationToken cancellationToken)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(SqlServerSchemaPreflightSession));
            if (page.Rows.Count == 0) return;
            try
            {
                await _owner.WritePreflightPageAsync(
                    _connection, _transaction, _definition, page, _options, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                throw new InvalidOperationException(
                    $"SQL Server destination '{_definition.DestinationName}' rejected projected values or constraints during schema preflight. No destination rows were changed.",
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

    private sealed class SqlServerSchemaPreflightBatchSession : IDbaTableCopySchemaPreflightBatchSession
    {
        private readonly SqlServerTableCopyAdapter _owner;
        private readonly SqlConnection _connection;
        private readonly SqlTransaction _transaction;
        private readonly IReadOnlyList<DbaTableCopyDefinition> _definitions;
        private readonly DbaTableCopyOptions _options;
        private bool _disposed;

        internal SqlServerSchemaPreflightBatchSession(
            SqlServerTableCopyAdapter owner,
            SqlConnection connection,
            SqlTransaction transaction,
            IReadOnlyList<DbaTableCopyDefinition> definitions,
            DbaTableCopyOptions options)
        {
            _owner = owner;
            _connection = connection;
            _transaction = transaction;
            _definitions = definitions;
            _options = options;
        }

        internal async Task InitializeAsync(CancellationToken cancellationToken)
        {
            if (!_options.ClearDestination) return;
            for (var index = _definitions.Count - 1; index >= 0; index--)
            {
                using var clear = new SqlCommand(
                    $"DELETE FROM {_owner.QuotePath(_definitions[index].DestinationName)}",
                    _connection,
                    _transaction)
                {
                    CommandTimeout = _owner.CommandTimeout
                };
                await clear.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        public async Task ValidatePageAsync(int definitionIndex, DataTable page, CancellationToken cancellationToken)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(SqlServerSchemaPreflightBatchSession));
            if ((uint)definitionIndex >= (uint)_definitions.Count) throw new ArgumentOutOfRangeException(nameof(definitionIndex));
            if (page.Rows.Count == 0) return;
            try
            {
                await _owner.WritePreflightPageAsync(
                    _connection,
                    _transaction,
                    _definitions[definitionIndex],
                    page,
                    _options,
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                throw new InvalidOperationException(
                    $"SQL Server destination '{_definitions[definitionIndex].DestinationName}' rejected coordinated projected values or constraints during schema preflight. No destination rows were changed.",
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

    private async Task WritePreflightPageAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        DbaTableCopyDefinition definition,
        DataTable page,
        DbaTableCopyOptions options,
        CancellationToken cancellationToken)
    {
        using var server = new SqlServer { ConnectionOptions = _connectionOptions, CommandTimeout = CommandTimeout };
        await server.WriteTableCopyRowsAsync(
            connection,
            transaction,
            page,
            NormalizeQuotedBulkDestinationTableName(definition.DestinationName),
            GetEffectiveBulkInsertOptions(options, externalTransaction: true, forceConstraints: true),
            options.BatchSize,
            options.BulkCopyTimeout,
            cancellationToken).ConfigureAwait(false);
    }
}
