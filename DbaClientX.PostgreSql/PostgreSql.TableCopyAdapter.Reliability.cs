using System.Data;
using System.Data.Common;
using DBAClientX.DataMovement;
using DBAClientX.Metadata;
using Npgsql;

namespace DBAClientX;

public sealed partial class PostgreSqlTableCopyAdapter : IDbaTableCopySchemaPreflightDestination
{
    internal const string PostgreSqlCheckpointDestinationIdentityQuery = @"SELECT current_database() || ':' || cls.oid::text
FROM pg_catalog.pg_class AS cls
WHERE cls.oid = to_regclass(@name)
  AND cls.relkind IN ('r', 'p')
  AND cls.relpersistence = 'p'";

    internal const string PostgreSqlSchemaPreflightDestinationQuery = @"
SELECT ns.nspname, cls.relname
FROM pg_catalog.pg_class AS cls
JOIN pg_catalog.pg_namespace AS ns ON ns.oid = cls.relnamespace
WHERE cls.oid = to_regclass(@name)
  AND cls.relkind IN ('r', 'p')
  AND cls.relpersistence = 'p'";

    /// <inheritdoc />
    public override bool SupportsAtomicCheckpoints => true;

    /// <inheritdoc />
    protected override DbConnection CreateCheckpointConnection() => new NpgsqlConnection(ConnectionString);

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
        IReadOnlyList<string> rawSegments = DbaIdentifierPath.SplitSegments(
            definition.DestinationName,
            DbaTableCopyProvider.PostgreSql);
        if (rawSegments.Count is < 1 or > 2)
        {
            throw new ArgumentException(
                "PostgreSQL table-copy destinations support table or schema.table names.",
                nameof(definition));
        }

        await using var connection = new NpgsqlConnection(ConnectionString);
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
        DataTable normalizedPage = DbaPostgreSqlBulkCopyNormalizer.NormalizePage(page, definition.DestinationName);
        using var normalizedPageToDispose = ReferenceEquals(normalizedPage, page) ? null : normalizedPage;
        DbaTableCopySchemaValidator.Validate(
            definition.DestinationName,
            normalizedPage.Columns.Cast<DataColumn>().Select(static column => column.ColumnName).ToArray(),
            columns,
            static name => name,
            requirePreservedIdentity: false,
            keepIdentity: true);
        await ValidateDestinationTypesAsync(
            connection,
            definition,
            normalizedPage,
            cancellationToken).ConfigureAwait(false);
    }

    private async Task ValidateDestinationTypesAsync(
        NpgsqlConnection connection,
        DbaTableCopyDefinition definition,
        DataTable normalizedPage,
        CancellationToken cancellationToken)
    {
        using DataTable sample = CreateTypeValidationSample(normalizedPage, definition.DestinationName);
        string temporaryTable = "dbaclientx_preflight_" + Guid.NewGuid().ToString("N");
        string quotedTemporaryTable = QuotePath(temporaryTable);
        IReadOnlyDictionary<string, string> destinationTypes = await ResolveDestinationTypeSqlAsync(
            connection,
            definition.DestinationName,
            sample.Columns.Cast<DataColumn>().Select(static column => column.ColumnName).ToArray(),
            cancellationToken).ConfigureAwait(false);
        string columnDefinitions = string.Join(", ", sample.Columns.Cast<DataColumn>()
            .Select(column => $"{QuoteExactIdentifier(column.ColumnName)} {destinationTypes[column.ColumnName]}"));
        cancellationToken.ThrowIfCancellationRequested();
        using NpgsqlTransaction transaction = connection.BeginTransaction();
        try
        {
            using (var create = new NpgsqlCommand(
                $"CREATE TEMP TABLE {quotedTemporaryTable} ({columnDefinitions}) ON COMMIT DROP",
                connection,
                transaction)
            {
                CommandTimeout = CommandTimeout
            })
            {
                await create.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            using var postgreSql = new PostgreSql { CommandTimeout = CommandTimeout };
            await postgreSql.WriteTableCopyRowsAsync(
                connection,
                transaction,
                sample,
                temporaryTable,
                CommandTimeout,
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            throw new InvalidOperationException(
                $"PostgreSQL destination '{definition.DestinationName}' rejected the projected CLR types during schema preflight. No destination rows were changed.",
                exception);
        }
        finally
        {
            transaction.Rollback();
        }
    }

    private async Task<IReadOnlyDictionary<string, string>> ResolveDestinationTypeSqlAsync(
        NpgsqlConnection connection,
        string destinationName,
        string[] columnNames,
        CancellationToken cancellationToken)
    {
        using var command = new NpgsqlCommand(@"
SELECT att.attname, pg_catalog.format_type(att.atttypid, att.atttypmod)
FROM pg_catalog.pg_attribute AS att
WHERE att.attrelid = to_regclass(@name)
  AND att.attname = ANY(@columns)
  AND att.attnum > 0
  AND NOT att.attisdropped", connection)
        {
            CommandTimeout = CommandTimeout
        };
        command.Parameters.AddWithValue("@name", QuotePath(destinationName));
        command.Parameters.AddWithValue("@columns", columnNames);
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        using NpgsqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            result[reader.GetString(0)] = reader.GetString(1);
        if (result.Count != columnNames.Length)
        {
            throw new InvalidOperationException(
                $"PostgreSQL destination '{destinationName}' did not expose exact type metadata for every projected column.");
        }
        return result;
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
        if (dataType == typeof(DateTime)) return new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);
        if (dataType == typeof(TimeSpan)) return TimeSpan.Zero;
        if (dataType == typeof(Guid)) return Guid.Empty;
        if (dataType == typeof(DateTimeOffset)) return new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);
        if (dataType == typeof(System.Net.IPAddress)) return System.Net.IPAddress.Loopback;
        if (dataType == typeof(System.Net.NetworkInformation.PhysicalAddress))
            return new System.Net.NetworkInformation.PhysicalAddress(new byte[6]);
#if NET472
        if (dataType == typeof(NpgsqlTypes.NpgsqlCidr))
            return new NpgsqlTypes.NpgsqlCidr(System.Net.IPAddress.Loopback, 32);
#else
        if (dataType == typeof(System.Net.IPNetwork))
            return new System.Net.IPNetwork(System.Net.IPAddress.Loopback, 32);
#endif
#if NET6_0_OR_GREATER
        if (dataType == typeof(DateOnly)) return new DateOnly(1970, 1, 1);
        if (dataType == typeof(TimeOnly)) return TimeOnly.MinValue;
#endif
        if (dataType == typeof(bool[])) return Array.Empty<bool>();
        if (dataType == typeof(short[])) return Array.Empty<short>();
        if (dataType == typeof(int[])) return Array.Empty<int>();
        if (dataType == typeof(long[])) return Array.Empty<long>();
        if (dataType == typeof(float[])) return Array.Empty<float>();
        if (dataType == typeof(double[])) return Array.Empty<double>();
        if (dataType == typeof(decimal[])) return Array.Empty<decimal>();
        if (dataType == typeof(string[])) return Array.Empty<string>();
        if (dataType == typeof(Guid[])) return Array.Empty<Guid>();
        if (dataType == typeof(DateTime[])) return Array.Empty<DateTime>();
        if (dataType == typeof(DateTimeOffset[])) return Array.Empty<DateTimeOffset>();
        if (dataType == typeof(TimeSpan[])) return Array.Empty<TimeSpan>();
        if (dataType == typeof(NpgsqlTypes.NpgsqlRange<int>)) return NpgsqlTypes.NpgsqlRange<int>.Empty;
        if (dataType == typeof(NpgsqlTypes.NpgsqlRange<long>)) return NpgsqlTypes.NpgsqlRange<long>.Empty;
        if (dataType == typeof(NpgsqlTypes.NpgsqlRange<decimal>)) return NpgsqlTypes.NpgsqlRange<decimal>.Empty;
        if (dataType == typeof(NpgsqlTypes.NpgsqlRange<DateTime>)) return NpgsqlTypes.NpgsqlRange<DateTime>.Empty;
        if (dataType == typeof(NpgsqlTypes.NpgsqlRange<int>[])) return Array.Empty<NpgsqlTypes.NpgsqlRange<int>>();
        if (dataType == typeof(NpgsqlTypes.NpgsqlRange<long>[])) return Array.Empty<NpgsqlTypes.NpgsqlRange<long>>();
        if (dataType == typeof(NpgsqlTypes.NpgsqlRange<decimal>[])) return Array.Empty<NpgsqlTypes.NpgsqlRange<decimal>>();
        if (dataType == typeof(NpgsqlTypes.NpgsqlRange<DateTime>[])) return Array.Empty<NpgsqlTypes.NpgsqlRange<DateTime>>();
#if NET6_0_OR_GREATER
        if (dataType == typeof(DateOnly[])) return Array.Empty<DateOnly>();
        if (dataType == typeof(TimeOnly[])) return Array.Empty<TimeOnly>();
        if (dataType == typeof(NpgsqlTypes.NpgsqlRange<DateOnly>)) return NpgsqlTypes.NpgsqlRange<DateOnly>.Empty;
        if (dataType == typeof(NpgsqlTypes.NpgsqlRange<DateOnly>[])) return Array.Empty<NpgsqlTypes.NpgsqlRange<DateOnly>>();
#endif
        throw new InvalidOperationException(
            $"PostgreSQL destination '{destinationName}' cannot safely preflight null-only column '{columnName}' with CLR type '{dataType.FullName}'. Declare an explicit column conversion.");
    }

    private static string QuoteExactIdentifier(string identifier)
    {
        if (identifier.IndexOfAny(new[] { ';', '\r', '\n', '\0' }) >= 0)
            throw new ArgumentException($"Identifier '{identifier}' contains unsupported characters.", nameof(identifier));
        return "\"" + identifier.Replace("\"", "\"\"") + "\"";
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
