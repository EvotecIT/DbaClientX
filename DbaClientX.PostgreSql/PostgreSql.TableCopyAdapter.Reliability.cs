using System.Data;
using System.Data.Common;
using DBAClientX.DataMovement;
using DBAClientX.Metadata;
using Npgsql;

namespace DBAClientX;

public sealed partial class PostgreSqlTableCopyAdapter : IDbaTableCopySchemaPreflightDestination, IDbaTableCopySchemaPreflightSessionDestination
{
    internal const string PostgreSqlCheckpointDestinationIdentityQuery = @"WITH RECURSIVE relation_tree AS (
    SELECT cls.oid, cls.relkind, cls.relpersistence
    FROM pg_catalog.pg_class AS cls
    WHERE cls.oid = to_regclass(@name)
    UNION ALL
    SELECT child.oid, child.relkind, child.relpersistence
    FROM pg_catalog.pg_inherits AS inheritance
    JOIN relation_tree AS parent ON parent.oid = inheritance.inhparent
    JOIN pg_catalog.pg_class AS child ON child.oid = inheritance.inhrelid
)
SELECT current_database() || ':' || root.oid::text
FROM relation_tree AS root
WHERE root.oid = to_regclass(@name)
  AND root.relkind IN ('r', 'p')
  AND root.relpersistence = 'p'
  AND NOT EXISTS (
      SELECT 1
      FROM relation_tree AS descendant
      WHERE descendant.relkind NOT IN ('r', 'p')
         OR descendant.relpersistence <> 'p'
  )";

    internal const string PostgreSqlSchemaPreflightDestinationQuery = @"
WITH RECURSIVE relation_tree AS (
    SELECT cls.oid, cls.relkind, cls.relpersistence
    FROM pg_catalog.pg_class AS cls
    WHERE cls.oid = to_regclass(@name)
    UNION ALL
    SELECT child.oid, child.relkind, child.relpersistence
    FROM pg_catalog.pg_inherits AS inheritance
    JOIN relation_tree AS parent ON parent.oid = inheritance.inhparent
    JOIN pg_catalog.pg_class AS child ON child.oid = inheritance.inhrelid
)
SELECT ns.nspname, root.relname
FROM pg_catalog.pg_class AS root
JOIN pg_catalog.pg_namespace AS ns ON ns.oid = root.relnamespace
WHERE root.oid = to_regclass(@name)
  AND root.relkind IN ('r', 'p')
  AND root.relpersistence = 'p'
  AND NOT EXISTS (
      SELECT 1
      FROM relation_tree AS descendant
      WHERE descendant.relkind NOT IN ('r', 'p')
         OR descendant.relpersistence <> 'p'
  )";

    internal const string PostgreSqlCheckpointStorageDurabilityQuery = @"SELECT cls.relpersistence
FROM pg_catalog.pg_class AS cls
WHERE cls.oid = to_regclass(@name)
  AND cls.relkind IN ('r', 'p')";

    internal const string PostgreSqlRollbackUnsafeTriggerQuery = @"WITH RECURSIVE relation_tree AS (
    SELECT cls.oid
    FROM pg_catalog.pg_class AS cls
    WHERE cls.oid = to_regclass(@name)
    UNION ALL
    SELECT inheritance.inhrelid
    FROM pg_catalog.pg_inherits AS inheritance
    JOIN relation_tree AS parent ON parent.oid = inheritance.inhparent
)
SELECT 1
FROM pg_catalog.pg_trigger
JOIN relation_tree ON relation_tree.oid = tgrelid
WHERE NOT tgisinternal
  AND tgenabled <> 'D'
  AND ((tgtype & 4) <> 0 OR (tgtype & 8) <> 0)
LIMIT 1";

    /// <inheritdoc />
    public override bool SupportsAtomicCheckpoints => true;

    /// <inheritdoc />
    protected override DbConnection CreateCheckpointConnection() => new NpgsqlConnection(ConnectionString);

    /// <inheritdoc />
    protected override async Task ValidateCheckpointSchemaAsync(
        DbConnection connection,
        CancellationToken cancellationToken)
    {
        using var command = new NpgsqlCommand(
            PostgreSqlCheckpointStorageDurabilityQuery,
            (NpgsqlConnection)connection)
        {
            CommandTimeout = CommandTimeout
        };
        command.Parameters.AddWithValue("@name", "\"DbaClientX_TableCopyCheckpoints\"");
        string? persistence = Convert.ToString(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
        ValidateCheckpointStorageDurability(persistence);
    }

    internal static void ValidateCheckpointStorageDurability(string? persistence)
    {
        if (!string.Equals(persistence, "p", StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                "Atomic PostgreSQL checkpoints require DbaClientX_TableCopyCheckpoints to be a permanent logged table in the active search path.");
        }
    }

    /// <inheritdoc />
    protected override async Task<string> ResolveCheckpointTableIdentityAsync(
        DbConnection connection,
        DbTransaction? transaction,
        DbaTableCopyDefinition definition,
        CancellationToken cancellationToken)
    {
        var segments = DbaIdentifierPath.SplitSegments(definition.DestinationName, DbaTableCopyProvider.PostgreSql);
        if (segments.Count is < 1 or > 2)
        {
            throw new ArgumentException(
                "PostgreSQL checkpoint destinations require a table name with an optional schema.",
                nameof(definition));
        }

        using var command = new NpgsqlCommand(
            PostgreSqlCheckpointDestinationIdentityQuery,
            (NpgsqlConnection)connection,
            (NpgsqlTransaction?)transaction)
        {
            CommandTimeout = CommandTimeout
        };
        command.Parameters.AddWithValue("@name", QuotePath(definition.DestinationName));
        var identity = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return identity as string ?? throw new InvalidOperationException(
            $"Checkpoint destination '{definition.DestinationName}' cannot be resolved to a PostgreSQL table.");
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
        IReadOnlyList<string> rawSegments = DbaIdentifierPath.SplitSegments(
            definition.DestinationName,
            DbaTableCopyProvider.PostgreSql);
        if (rawSegments.Count is < 1 or > 2)
        {
            throw new ArgumentException(
                "PostgreSQL table-copy destinations support table or schema.table names.",
                nameof(definition));
        }

        var connection = new NpgsqlConnection(ConnectionString);
        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
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
                        $"PostgreSQL destination '{definition.DestinationName}' could not be resolved for schema preflight.");
                }

                schema = reader.GetString(0);
                table = reader.GetString(1);
            }

            using var postgreSql = new PostgreSql { CommandTimeout = CommandTimeout };
            var columns = await postgreSql.GetTableCopyColumnsAsync(
                connection,
                schema,
                table,
                cancellationToken).ConfigureAwait(false);
            DataTable normalizedPage = DbaPostgreSqlBulkCopyNormalizer.NormalizePage(firstPage, definition.DestinationName);
            using var normalizedPageToDispose = ReferenceEquals(normalizedPage, firstPage) ? null : normalizedPage;
            DbaTableCopySchemaValidator.Validate(
                definition.DestinationName,
                normalizedPage.Columns.Cast<DataColumn>().Select(static column => column.ColumnName).ToArray(),
                columns,
                static name => name,
                requirePreservedIdentity: false,
                keepIdentity: true);
            if (options.ClearDestination)
            {
                ValidateRollbackSafeGeneratorProjection(
                    definition.DestinationName,
                    normalizedPage.Columns.Cast<DataColumn>().Select(static column => column.ColumnName).ToArray(),
                    columns);
                await ValidateRollbackSafeTriggersAsync(
                    connection,
                    definition.DestinationName,
                    cancellationToken).ConfigureAwait(false);
            }
            var session = new PostgreSqlSchemaPreflightSession(
                this,
                connection,
                connection.BeginTransaction(),
                definition,
                options);
            try
            {
                await session.InitializeAsync(normalizedPage, cancellationToken).ConfigureAwait(false);
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
        var supplied = new HashSet<string>(projectedColumns, StringComparer.Ordinal);
        DbaColumnInfo? generator = destinationColumns.FirstOrDefault(column =>
            !supplied.Contains(column.Name) &&
            (column.IsIdentity == true ||
             column.DefaultExpression?.IndexOf("nextval", StringComparison.OrdinalIgnoreCase) >= 0));
        if (generator == null) return;

        throw new InvalidOperationException(
            $"PostgreSQL destination '{tableName}' omits generator-backed column '{generator.Name}'. " +
            "ClearDestination cannot safely preflight this projection because sequence advances are not rolled back. " +
            "Project an explicit value for the column or copy without ClearDestination.");
    }

    private async Task ValidateRollbackSafeTriggersAsync(
        NpgsqlConnection connection,
        string destinationName,
        CancellationToken cancellationToken)
    {
        using var command = new NpgsqlCommand(PostgreSqlRollbackUnsafeTriggerQuery, connection)
        {
            CommandTimeout = CommandTimeout
        };
        command.Parameters.AddWithValue("@name", QuotePath(destinationName));
        if (await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) == null) return;

        throw new InvalidOperationException(
            $"PostgreSQL destination '{destinationName}' has an enabled INSERT or DELETE trigger. " +
            "ClearDestination cannot safely preflight trigger side effects because sequence advances and external actions are not rolled back. " +
            "Disable or remove the trigger for the copy, or copy without ClearDestination.");
    }

    private sealed class PostgreSqlSchemaPreflightSession : IDbaTableCopySchemaPreflightSession
    {
        private readonly PostgreSqlTableCopyAdapter _owner;
        private readonly NpgsqlConnection _connection;
        private readonly NpgsqlTransaction _transaction;
        private readonly DbaTableCopyDefinition _definition;
        private readonly DbaTableCopyOptions _options;
        private bool _disposed;

        internal PostgreSqlSchemaPreflightSession(
            PostgreSqlTableCopyAdapter owner,
            NpgsqlConnection connection,
            NpgsqlTransaction transaction,
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
            if (!_options.ClearDestination) return;

            using var clear = new NpgsqlCommand(
                $"DELETE FROM {_owner.QuotePath(_definition.DestinationName)}",
                _connection,
                _transaction)
            {
                CommandTimeout = _owner.CommandTimeout
            };
            await clear.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            await ValidatePageCoreAsync(page, cancellationToken).ConfigureAwait(false);
        }

        public async Task ValidatePageAsync(DataTable page, CancellationToken cancellationToken)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(PostgreSqlSchemaPreflightSession));
            DataTable normalized = DbaPostgreSqlBulkCopyNormalizer.NormalizePage(page, _definition.DestinationName);
            using var normalizedToDispose = ReferenceEquals(normalized, page) ? null : normalized;
            await ValidatePageCoreAsync(normalized, cancellationToken).ConfigureAwait(false);
        }

        private async Task ValidatePageCoreAsync(DataTable page, CancellationToken cancellationToken)
        {
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
                    $"PostgreSQL destination '{_definition.DestinationName}' rejected the projected CLR types, values, or constraints during schema preflight. No destination rows were changed.",
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

    /// <inheritdoc />
    protected override async Task WriteTransactionalPageAsync(
        DbConnection connection,
        DbTransaction transaction,
        DbaTableCopyDefinition definition,
        DataTable page,
        DbaTableCopyOptions options,
        CancellationToken cancellationToken)
    {
        var bulkPage = DbaPostgreSqlBulkCopyNormalizer.NormalizePage(page, definition.DestinationName);
        using var bulkPageToDispose = ReferenceEquals(bulkPage, page) ? null : bulkPage;
        using var postgreSql = new PostgreSql { CommandTimeout = CommandTimeout };
        await postgreSql.WriteTableCopyRowsAsync(
            (NpgsqlConnection)connection,
            (NpgsqlTransaction)transaction,
            bulkPage,
            DbaPostgreSqlBulkCopyNormalizer.NormalizeDestinationTableName(definition.DestinationName),
            options.BulkCopyTimeout,
            cancellationToken).ConfigureAwait(false);
    }
}
