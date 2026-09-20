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
        await ValidateDestinationTypesAsync(connection, definition, page, cancellationToken).ConfigureAwait(false);
    }

    private async Task ValidateDestinationTypesAsync(
        MySqlConnection connection,
        DbaTableCopyDefinition definition,
        DataTable page,
        CancellationToken cancellationToken)
    {
        using DataTable sample = CreateTypeValidationSample(page, definition.DestinationName);
        string temporaryTable = "dbaclientx_preflight_" + Guid.NewGuid().ToString("N");
        string quotedTemporaryTable = QuotePath(temporaryTable);
        string projectedColumns = string.Join(", ", page.Columns.Cast<DataColumn>()
            .Select(static column => QuoteExactMySqlIdentifier(column.ColumnName)));
        string destinationTable = QuotePath(definition.DestinationName);

        try
        {
            await using (var create = new MySqlCommand(
                $"CREATE TEMPORARY TABLE {quotedTemporaryTable} AS SELECT {projectedColumns} FROM {destinationTable} WHERE 1 = 0",
                connection)
            {
                CommandTimeout = CommandTimeout
            })
            {
                await create.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            using MySqlTransaction transaction = connection.BeginTransaction();
            try
            {
                using var mySql = new MySql { CommandTimeout = CommandTimeout };
                await mySql.WriteTableCopyRowsAsync(
                    connection,
                    transaction,
                    sample,
                    quotedTemporaryTable,
                    batchSize: null,
                    bulkCopyTimeout: CommandTimeout,
                    cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                transaction.Rollback();
            }
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            throw new InvalidOperationException(
                $"MySQL destination '{definition.DestinationName}' rejected the projected CLR values during schema preflight. No destination rows were changed.",
                exception);
        }
        finally
        {
            await using var drop = new MySqlCommand(
                $"DROP TEMPORARY TABLE IF EXISTS {quotedTemporaryTable}",
                connection)
            {
                CommandTimeout = CommandTimeout
            };
            await drop.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }

    private static DataTable CreateTypeValidationSample(DataTable page, string destinationName)
    {
        var sample = page.Clone();
        var values = new object[page.Columns.Count];
        for (var index = 0; index < page.Columns.Count; index++)
        {
            DataColumn column = page.Columns[index];
            object? value = page.Rows.Cast<DataRow>()
                .Select(row => row[index])
                .FirstOrDefault(static candidate => candidate is not null and not DBNull);
            values[index] = value ?? CreateRepresentativeValue(column.DataType, destinationName, column.ColumnName);
        }
        sample.Rows.Add(values);
        return sample;
    }

    private static object CreateRepresentativeValue(Type dataType, string destinationName, string columnName)
    {
        if (dataType == typeof(string)) return string.Empty;
        if (dataType == typeof(byte[])) return Array.Empty<byte>();
        if (dataType == typeof(char[])) return Array.Empty<char>();
        if (dataType == typeof(bool)) return false;
        if (dataType == typeof(byte)) return (byte)0;
        if (dataType == typeof(sbyte)) return (sbyte)0;
        if (dataType == typeof(short)) return (short)0;
        if (dataType == typeof(ushort)) return (ushort)0;
        if (dataType == typeof(int)) return 0;
        if (dataType == typeof(uint)) return 0U;
        if (dataType == typeof(long)) return 0L;
        if (dataType == typeof(ulong)) return 0UL;
        if (dataType == typeof(float)) return 0F;
        if (dataType == typeof(double)) return 0D;
        if (dataType == typeof(decimal)) return 0M;
        if (dataType == typeof(char)) return '\0';
        if (dataType == typeof(DateTime)) return new DateTime(1970, 1, 1);
        if (dataType == typeof(TimeSpan)) return TimeSpan.Zero;
        if (dataType == typeof(Guid)) return Guid.Empty;
        if (dataType == typeof(DateTimeOffset)) return new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);
#if NET6_0_OR_GREATER
        if (dataType == typeof(DateOnly)) return new DateOnly(1970, 1, 1);
        if (dataType == typeof(TimeOnly)) return TimeOnly.MinValue;
#endif
        throw new InvalidOperationException(
            $"MySQL destination '{destinationName}' cannot safely preflight null-only column '{columnName}' with CLR type '{dataType.FullName}'. Declare an explicit column conversion.");
    }

    private static string QuoteExactMySqlIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new ArgumentException("Identifier cannot be null or whitespace.", nameof(identifier));
        if (identifier.IndexOfAny(new[] { ';', '\r', '\n', '\0' }) >= 0)
            throw new ArgumentException($"Identifier '{identifier}' contains unsupported characters.", nameof(identifier));
        return "`" + identifier.Replace("`", "``") + "`";
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
