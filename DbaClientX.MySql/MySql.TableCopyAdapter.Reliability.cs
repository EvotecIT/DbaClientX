using System.Data;
using System.Data.Common;
using DBAClientX.DataMovement;
using DBAClientX.Metadata;
using MySqlConnector;

namespace DBAClientX;

public sealed partial class MySqlTableCopyAdapter : IDbaTableCopySchemaPreflightDestination, IDbaTableCopySchemaPreflightSessionDestination
{
    internal const string MySqlRollbackUnsafeTriggerQuery = @"SELECT 1
FROM INFORMATION_SCHEMA.TRIGGERS
WHERE ((@@lower_case_table_names = 0 AND BINARY TRIGGER_SCHEMA = BINARY @database AND BINARY EVENT_OBJECT_TABLE = BINARY @table)
       OR (@@lower_case_table_names <> 0 AND TRIGGER_SCHEMA = @database AND EVENT_OBJECT_TABLE = @table))
  AND EVENT_MANIPULATION IN ('INSERT', 'DELETE')
LIMIT 1";

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
            @"SELECT ENGINE
FROM INFORMATION_SCHEMA.TABLES
WHERE (@@lower_case_table_names <> 0 AND TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'DbaClientX_TableCopyCheckpoints')
   OR (@@lower_case_table_names = 0 AND BINARY TABLE_SCHEMA = BINARY DATABASE() AND BINARY TABLE_NAME = BINARY 'DbaClientX_TableCopyCheckpoints')",
            (MySqlConnection)connection)
        {
            CommandTimeout = CommandTimeout
        };
        var engine = Convert.ToString(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
        ValidateCheckpointStorageEngine(engine);
    }

    internal static void ValidateCheckpointStorageEngine(string? engine)
    {
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
            @"SELECT TABLE_SCHEMA, TABLE_NAME, ENGINE
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
  AND ((@@lower_case_table_names = 0 AND BINARY TABLE_SCHEMA = BINARY @database AND BINARY TABLE_NAME = BINARY @table)
       OR (@@lower_case_table_names <> 0 AND TABLE_SCHEMA = @database AND TABLE_NAME = @table))",
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

        var identity = CreateTableIdentity(reader.GetString(0), reader.GetString(1));
        var engine = reader.IsDBNull(2) ? null : reader.GetString(2);
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
        await using IDbaTableCopySchemaPreflightSession session = await OpenSchemaPreflightSessionAsync(
            definition,
            page,
            options,
            cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IDbaTableCopySchemaPreflightSession> OpenSchemaPreflightSessionAsync(
        DbaTableCopyDefinition definition,
        DataTable firstPage,
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

        var connection = new MySqlConnection(ConnectionString);
        try
        {
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
                firstPage.Columns.Cast<DataColumn>().Select(static column => column.ColumnName).ToArray(),
                columns,
                static name => DbaIdentifierPath.UnquoteSegment(name, DbaTableCopyProvider.MySql).ToUpperInvariant(),
                requirePreservedIdentity: false,
                keepIdentity: true);
            if (options.ClearDestination)
            {
                ValidateRollbackSafeGeneratorProjection(
                    definition.DestinationName,
                    firstPage.Columns.Cast<DataColumn>().Select(static column => column.ColumnName).ToArray(),
                    columns);
                await ValidateRollbackSafeTriggersAsync(
                    connection,
                    database,
                    segments[segments.Length - 1],
                    definition.DestinationName,
                    cancellationToken).ConfigureAwait(false);
            }
            await EnsureTransactionalPreflightDestinationAsync(
                connection,
                definition.DestinationName,
                database,
                segments[segments.Length - 1],
                cancellationToken).ConfigureAwait(false);
            var session = new MySqlSchemaPreflightSession(
                this,
                connection,
                connection.BeginTransaction(),
                definition,
                options);
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

    internal static void ValidateRollbackSafeGeneratorProjection(
        string tableName,
        IReadOnlyCollection<string> projectedColumns,
        IReadOnlyList<DbaColumnInfo> destinationColumns)
    {
        var supplied = new HashSet<string>(
            projectedColumns.Select(name =>
                DbaIdentifierPath.UnquoteSegment(name, DbaTableCopyProvider.MySql).ToUpperInvariant()),
            StringComparer.Ordinal);
        DbaColumnInfo? generator = destinationColumns.FirstOrDefault(column =>
            !supplied.Contains(DbaIdentifierPath.UnquoteSegment(column.Name, DbaTableCopyProvider.MySql).ToUpperInvariant()) &&
            column.IsIdentity == true);
        if (generator == null) return;

        throw new InvalidOperationException(
            $"MySQL destination '{tableName}' omits auto-increment column '{generator.Name}'. " +
            "ClearDestination cannot safely preflight this projection because auto-increment advances are not rolled back. " +
            "Project an explicit value for the column or copy without ClearDestination.");
    }

    private async Task ValidateRollbackSafeTriggersAsync(
        MySqlConnection connection,
        string database,
        string table,
        string destinationName,
        CancellationToken cancellationToken)
    {
        await using var command = new MySqlCommand(MySqlRollbackUnsafeTriggerQuery, connection)
        {
            CommandTimeout = CommandTimeout
        };
        command.Parameters.AddWithValue("@database", database);
        command.Parameters.AddWithValue("@table", table);
        if (await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) == null) return;

        throw new InvalidOperationException(
            $"MySQL destination '{destinationName}' has an INSERT or DELETE trigger. " +
            "ClearDestination cannot safely preflight trigger side effects because generated values and external actions may not be rolled back. " +
            "Remove the trigger for the copy, or copy without ClearDestination.");
    }

    private sealed class MySqlSchemaPreflightSession : IDbaTableCopySchemaPreflightSession
    {
        private readonly MySqlTableCopyAdapter _owner;
        private readonly MySqlConnection _connection;
        private readonly MySqlTransaction _transaction;
        private readonly DbaTableCopyDefinition _definition;
        private readonly DbaTableCopyOptions _options;
        private bool _disposed;

        internal MySqlSchemaPreflightSession(
            MySqlTableCopyAdapter owner,
            MySqlConnection connection,
            MySqlTransaction transaction,
            DbaTableCopyDefinition definition,
            DbaTableCopyOptions options)
        {
            _owner = owner;
            _connection = connection;
            _transaction = transaction;
            _definition = definition;
            _options = options;
        }

        internal async Task InitializeAsync(DataTable page, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (_options.ClearDestination)
            {
                await using var clear = new MySqlCommand(
                    $"DELETE FROM {_owner.QuotePath(_definition.DestinationName)}",
                    _connection,
                    _transaction)
                {
                    CommandTimeout = _owner.CommandTimeout
                };
                await clear.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await ValidatePageAsync(page, cancellationToken).ConfigureAwait(false);
        }

        public async Task ValidatePageAsync(DataTable page, CancellationToken cancellationToken)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(MySqlSchemaPreflightSession));
            if (page.Rows.Count == 0) return;
            try
            {
                await _owner.WriteTransactionalPageAsync(
                    _connection,
                    _transaction,
                    _definition,
                    page,
                    _options,
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                throw new InvalidOperationException(
                    $"MySQL destination '{_definition.DestinationName}' rejected the projected values or constraints during schema preflight. No destination rows were changed.",
                    exception);
            }
        }

        public ValueTask DisposeAsync()
        {
            if (_disposed) return default;
            _disposed = true;
            try
            {
                _transaction.Rollback();
            }
            finally
            {
                _transaction.Dispose();
                _connection.Dispose();
            }
            return default;
        }
    }

    private async Task EnsureTransactionalPreflightDestinationAsync(
        MySqlConnection connection,
        string destinationName,
        string database,
        string table,
        CancellationToken cancellationToken)
    {
        await using var command = new MySqlCommand(
            @"SELECT ENGINE
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
  AND ((@@lower_case_table_names = 0 AND BINARY TABLE_SCHEMA = BINARY @database AND BINARY TABLE_NAME = BINARY @table)
       OR (@@lower_case_table_names <> 0 AND TABLE_SCHEMA = @database AND TABLE_NAME = @table))",
            connection)
        {
            CommandTimeout = CommandTimeout
        };
        command.Parameters.AddWithValue("@database", database);
        command.Parameters.AddWithValue("@table", table);
        string? engine = Convert.ToString(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
        if (!string.Equals(engine, "InnoDB", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"MySQL destination '{destinationName}' must use InnoDB so schema preflight writes can be rolled back; found '{engine ?? "unknown"}'.");
        }
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
        using DataTable? normalizedPage = NormalizeBulkPage(page);
        using var mySql = new MySql { CommandTimeout = CommandTimeout };
        await mySql.WriteTableCopyRowsAsync(
            (MySqlConnection)connection,
            (MySqlTransaction)transaction,
            normalizedPage ?? page,
            NormalizeQuotedBulkDestinationTableName(definition.DestinationName),
            options.BatchSize,
            options.BulkCopyTimeout,
            cancellationToken).ConfigureAwait(false);
    }
}
