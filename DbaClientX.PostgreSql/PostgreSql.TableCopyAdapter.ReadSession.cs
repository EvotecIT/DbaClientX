using System.Data;
using DBAClientX.DataMovement;
using Npgsql;
using NpgsqlTypes;

namespace DBAClientX;

public sealed partial class PostgreSqlTableCopyAdapter : IDbaTableCopyContentValueNormalizer
{
    private NpgsqlConnection? _readConnection;
    private NpgsqlTransaction? _readTransaction;
    private int _readSessionActive;

    /// <summary>Consistency used for an engine-owned source read session.</summary>
    public DbaTableCopyReadConsistency ReadConsistency { get; set; }

    /// <inheritdoc />
    public Task<IDisposable?> OpenReadSessionAsync(CancellationToken cancellationToken = default)
    {
        if (ReadConsistency == DbaTableCopyReadConsistency.CallerManaged)
            return Task.FromResult<IDisposable?>(null);
        throw new InvalidOperationException(
            "PostgreSQL consistent read sessions require the source table definitions so DbaClientX can reject foreign tables whose remote data is not protected by the local transaction. Use the table-copy engine or the definition-aware session overload.");
    }

    /// <inheritdoc />
    public async Task<IDisposable?> OpenReadSessionAsync(
        IReadOnlyList<DbaTableCopyDefinition> definitions,
        CancellationToken cancellationToken = default)
    {
        if (ReadConsistency == DbaTableCopyReadConsistency.CallerManaged) return null;
        if (definitions == null) throw new ArgumentNullException(nameof(definitions));
        if (ReadConsistency is not (DbaTableCopyReadConsistency.Snapshot or DbaTableCopyReadConsistency.Serializable))
            throw new ArgumentOutOfRangeException(nameof(ReadConsistency));
        if (Interlocked.CompareExchange(ref _readSessionActive, 1, 0) != 0)
            throw new InvalidOperationException("A table-copy read session is already active on this adapter.");
        try
        {
            _readConnection = new NpgsqlConnection(ConnectionString);
            await _readConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
            IsolationLevel isolation = ReadConsistency == DbaTableCopyReadConsistency.Snapshot
                ? IsolationLevel.RepeatableRead
                : IsolationLevel.Serializable;
            cancellationToken.ThrowIfCancellationRequested();
            _readTransaction = _readConnection.BeginTransaction(isolation);
            await ValidateConsistentSourceTablesAsync(definitions, cancellationToken).ConfigureAwait(false);
            return new ReadSessionLease(this);
        }
        catch
        {
            CloseReadSession();
            throw;
        }
    }

    private async Task ValidateConsistentSourceTablesAsync(
        IReadOnlyList<DbaTableCopyDefinition> definitions,
        CancellationToken cancellationToken)
    {
        var validated = new HashSet<string>(StringComparer.Ordinal);
        foreach (DbaTableCopyDefinition definition in definitions)
        {
            IReadOnlyList<string> segments = DbaIdentifierPath.SplitSegments(
                definition.SourceName,
                DbaTableCopyProvider.PostgreSql);
            if (segments.Count is < 1 or > 2)
            {
                throw new ArgumentException(
                    "PostgreSQL consistent-read sources require a table name with an optional schema.",
                    nameof(definitions));
            }

            // Access the relation first so PostgreSQL retains an ACCESS SHARE lock through the
            // transaction and closes the validation/use race with concurrent DDL.
            const string sourceProbeSavepoint = "dbaclientx_source_probe";
            await ExecuteReadSessionCommandAsync(
                $"SAVEPOINT {sourceProbeSavepoint}",
                cancellationToken).ConfigureAwait(false);
            using (NpgsqlCommand metadataLock = CreateReadCommand(
                $"SELECT 1 FROM {QuotePath(definition.SourceName)} LIMIT 0"))
            {
                try
                {
                    await metadataLock.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (Exception exception) when (TreatMissingTablesAsEmpty && IsMissingTableException(exception))
                {
                    await ExecuteReadSessionCommandAsync(
                        $"ROLLBACK TO SAVEPOINT {sourceProbeSavepoint}",
                        cancellationToken).ConfigureAwait(false);
                    await ExecuteReadSessionCommandAsync(
                        $"RELEASE SAVEPOINT {sourceProbeSavepoint}",
                        cancellationToken).ConfigureAwait(false);
                    continue;
                }
            }
            await ExecuteReadSessionCommandAsync(
                $"RELEASE SAVEPOINT {sourceProbeSavepoint}",
                cancellationToken).ConfigureAwait(false);

            using NpgsqlCommand command = CreateReadCommand(PostgreSqlConsistentSourceRelationQuery);
            command.Parameters.AddWithValue("@name", QuotePath(definition.SourceName));
            using NpgsqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                throw new InvalidOperationException(
                    $"PostgreSQL source '{definition.SourceName}' could not be resolved for consistent-read validation.");
            }

            string identity = reader.GetString(0);
            if (!validated.Add(identity)) continue;
            ValidateConsistentSourceRelationKind(
                definition.SourceName,
                reader.GetString(1),
                reader.GetBoolean(2),
                ReadConsistency);
        }
    }

    internal const string PostgreSqlConsistentSourceRelationQuery = @"
WITH RECURSIVE root AS (
    SELECT cls.oid, cls.relkind
    FROM pg_catalog.pg_class AS cls
    WHERE cls.oid = to_regclass(@name)
), view_dependencies AS (
    SELECT root.oid
    FROM root
    UNION
    SELECT dependency.refobjid
    FROM view_dependencies AS owner
    JOIN pg_catalog.pg_rewrite AS rewrite ON rewrite.ev_class = owner.oid
    JOIN pg_catalog.pg_depend AS dependency
      ON dependency.classid = 'pg_catalog.pg_rewrite'::regclass
     AND dependency.objid = rewrite.oid
     AND dependency.refclassid = 'pg_catalog.pg_class'::regclass
    JOIN pg_catalog.pg_class AS referenced ON referenced.oid = dependency.refobjid
    WHERE dependency.refobjid <> owner.oid
      AND referenced.relkind IN ('r', 'p', 'f', 'v', 'm')
), relation_tree AS (
    SELECT view_dependencies.oid
    FROM view_dependencies
    UNION
    SELECT inheritance.inhrelid
    FROM pg_catalog.pg_inherits AS inheritance
    JOIN relation_tree AS parent ON inheritance.inhparent = parent.oid
)
SELECT root.oid::text,
       root.relkind::text,
       EXISTS (
           SELECT 1
           FROM relation_tree AS tree
           JOIN pg_catalog.pg_class AS descendant ON descendant.oid = tree.oid
           WHERE descendant.relkind = 'f'
       )
FROM root";

    internal static void ValidateConsistentSourceRelationKind(
        string sourceName,
        string relationKind,
        bool containsForeignRelation,
        DbaTableCopyReadConsistency consistency)
    {
        if (!containsForeignRelation && !string.Equals(relationKind, "f", StringComparison.Ordinal)) return;
        throw new InvalidOperationException(
            $"PostgreSQL {consistency} read consistency does not support foreign source table dependencies for '{sourceName}' because its relation, partition tree, or view dependency graph contains a foreign table whose remote data is not protected by the local transaction and cannot provide a stable remote snapshot.");
    }

    private NpgsqlCommand CreateReadCommand(string query)
        => new(query, _readConnection ?? throw new InvalidOperationException("No read session is active."), _readTransaction)
        {
            CommandTimeout = CommandTimeout
        };

    private async Task ExecuteReadSessionCommandAsync(string query, CancellationToken cancellationToken)
    {
        using NpgsqlCommand command = CreateReadCommand(query);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    protected override Task<DataTable> ExecuteBoundedPageCoreAsync(string query, IReadOnlyDictionary<string, object?> parameters, long? maxBytes, CancellationToken cancellationToken)
        => ExecutePostgreSqlPageAsync(query, parameters, maxBytes, cancellationToken);

    private async Task<DataTable> ExecutePostgreSqlPageAsync(string query, IReadOnlyDictionary<string, object?> parameters, long? maxBytes, CancellationToken cancellationToken)
    {
        using NpgsqlConnection? owned = _readConnection == null ? new NpgsqlConnection(ConnectionString) : null;
        NpgsqlConnection connection = _readConnection ?? owned!;
        if (owned != null) await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        using NpgsqlCommand command = _readConnection == null
            ? new NpgsqlCommand(query, connection) { CommandTimeout = CommandTimeout }
            : CreateReadCommand(query);
        foreach (KeyValuePair<string, object?> parameter in parameters)
            command.Parameters.AddWithValue(parameter.Key, GetPageParameterValue(parameter.Value));
        using CancellationTokenRegistration registration = cancellationToken.Register(static state => ((NpgsqlCommand)state!).Cancel(), command);
        using NpgsqlDataReader reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken).ConfigureAwait(false);
        ValidateNumericColumns(reader);
        bool[] intervalColumns = Enumerable.Range(0, reader.FieldCount)
            .Select(ordinal => string.Equals(reader.GetDataTypeName(ordinal), "interval", StringComparison.OrdinalIgnoreCase))
            .ToArray();
        Func<int, long?>? fieldPayloadBytes = maxBytes.HasValue
            ? ordinal => ValidateBoundedFieldType(reader, ordinal)
            : null;
        return await DbaTableCopyPageReader.ReadAsync(
            reader,
            maxBytes,
            fieldPayloadBytes,
            readFieldValue: ordinal => intervalColumns[ordinal]
                ? NormalizeInterval(reader.GetFieldValue<NpgsqlInterval>(ordinal))
                : NormalizeProviderValue(reader.GetValue(ordinal)),
            normalizedFieldType: ordinal => intervalColumns[ordinal]
                ? typeof(DbaCalendarInterval)
                : GetNormalizedFieldType(reader.GetFieldType(ordinal)),
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    private static void ValidateNumericColumns(NpgsqlDataReader reader)
    {
        var schema = reader.GetColumnSchema();
        for (var ordinal = 0; ordinal < reader.FieldCount; ordinal++)
        {
            if (!IsPostgreSqlNumeric(reader.GetDataTypeName(ordinal))) continue;
            ValidateNumericShape(
                reader.GetName(ordinal),
                schema[ordinal].NumericPrecision,
                schema[ordinal].NumericScale);
        }
    }

    internal static void ValidateNumericShape(string columnName, int? precision, int? scale)
    {
        long effectivePrecision = precision.GetValueOrDefault() + Math.Max(0L, -(long)scale.GetValueOrDefault());
        if (precision is > 0 and <= 28 && scale is >= -27 and <= 28 && effectivePrecision <= 28) return;

        string shape = precision.HasValue && scale.HasValue
            ? $"numeric({precision.Value},{scale.Value})"
            : "unconstrained numeric";
        throw new NotSupportedException(
            $"PostgreSQL table-copy column '{columnName}' uses {shape}, which can exceed System.Decimal precision. " +
            "Project it to text or apply an explicit provider-neutral column conversion before copying.");
    }

    private static bool IsPostgreSqlNumeric(string dataTypeName)
    {
        string normalized = dataTypeName.Trim().ToLowerInvariant();
        return normalized is "numeric" or "decimal" ||
               normalized.StartsWith("numeric(", StringComparison.Ordinal) ||
               normalized.StartsWith("decimal(", StringComparison.Ordinal);
    }

    internal static object NormalizeProviderValue(object value)
    {
#if NET472
        if (value is NpgsqlCidr cidr) return new DbaIpNetwork(cidr.Address, cidr.Netmask);
#else
        if (value is System.Net.IPNetwork network) return new DbaIpNetwork(network.BaseAddress, network.PrefixLength);
#endif
        return value;
    }

    internal static DbaCalendarInterval NormalizeInterval(NpgsqlInterval value)
        => new(value.Months, value.Days, value.Time);

    /// <inheritdoc />
    public object? NormalizeContentValue(object value)
    {
        return value switch
        {
            NpgsqlPoint point => NormalizePoint(point),
            NpgsqlLine line => new object?[] { "PostgreSQL line", line.A, line.B, line.C },
            NpgsqlLSeg segment => new object?[] { "PostgreSQL line segment", NormalizePoint(segment.Start), NormalizePoint(segment.End) },
            NpgsqlBox box => new object?[] { "PostgreSQL box", NormalizePoint(box.UpperRight), NormalizePoint(box.LowerLeft) },
            NpgsqlPath path => new object?[] { "PostgreSQL path", path.Open, NormalizePoints(path) },
            NpgsqlPolygon polygon => new object?[] { "PostgreSQL polygon", NormalizePoints(polygon) },
            NpgsqlCircle circle => new object?[] { "PostgreSQL circle", NormalizePoint(circle.Center), circle.Radius },
            NpgsqlRange<int> range => NormalizeRange(range),
            NpgsqlRange<long> range => NormalizeRange(range),
            NpgsqlRange<decimal> range => NormalizeRange(range),
            NpgsqlRange<DateTime> range => NormalizeRange(range),
#if NET6_0_OR_GREATER
            NpgsqlRange<DateOnly> range => NormalizeRange(range),
#endif
            _ => value
        };
    }

    private static object?[] NormalizePoint(NpgsqlPoint point)
        => new object?[] { "PostgreSQL point", point.X, point.Y };

    private static object?[] NormalizePoints(IEnumerable<NpgsqlPoint> points)
        => points.Select(NormalizePoint).Cast<object?>().ToArray();

    private static object?[] NormalizeRange<T>(NpgsqlRange<T> range)
    {
        return new object?[]
        {
            "PostgreSQL range",
            range.IsEmpty,
            range.LowerBoundInfinite,
            range.UpperBoundInfinite,
            range.LowerBoundIsInclusive,
            range.UpperBoundIsInclusive,
            range.LowerBoundInfinite ? DBNull.Value : range.LowerBound,
            range.UpperBoundInfinite ? DBNull.Value : range.UpperBound
        };
    }

    internal static Type GetNormalizedFieldType(Type providerType)
    {
#if NET472
        if (providerType == typeof(NpgsqlCidr)) return typeof(DbaIpNetwork);
#else
        if (providerType == typeof(System.Net.IPNetwork)) return typeof(DbaIpNetwork);
#endif
        return providerType;
    }

    internal static object GetPageParameterValue(object? value)
    {
        if (value is DbaCalendarInterval interval)
            return new NpgsqlInterval(interval.Months, interval.Days, interval.Microseconds);
        if (value is not DbaIpNetwork network) return value ?? DBNull.Value;
#if NET472
        return new NpgsqlCidr(network.Address, checked((byte)network.PrefixLength));
#else
        return new System.Net.IPNetwork(network.Address, network.PrefixLength);
#endif
    }

    private static long? ValidateBoundedFieldType(NpgsqlDataReader reader, int ordinal)
    {
        Type type = reader.GetFieldType(ordinal);
        if (type == typeof(string) || type == typeof(byte[]) ||
            type.IsPrimitive || type.IsEnum ||
            type == typeof(decimal) || type == typeof(Guid) ||
            type == typeof(DateTime) || type == typeof(DateTimeOffset) || type == typeof(TimeSpan) ||
            type == typeof(System.Net.IPAddress) || type == typeof(System.Net.NetworkInformation.PhysicalAddress))
        {
            return null;
        }
        if (string.Equals(reader.GetDataTypeName(ordinal), "interval", StringComparison.OrdinalIgnoreCase)) return null;
#if NET6_0_OR_GREATER
        if (type == typeof(DateOnly) || type == typeof(TimeOnly)) return null;
#endif
#if NET472
        if (type == typeof(NpgsqlCidr)) return null;
#else
        if (type == typeof(System.Net.IPNetwork)) return null;
#endif
        throw new NotSupportedException(
            $"Bounded PostgreSQL table-copy pages do not materialize variable-size native type '{reader.GetDataTypeName(ordinal)}' ({type.FullName}). Project it to text or binary, or omit MaxPageBytes.");
    }

    private void CloseReadSession()
    {
        try
        {
            _readTransaction?.Dispose();
        }
        finally
        {
            _readTransaction = null;
            _readConnection?.Dispose();
            _readConnection = null;
            Volatile.Write(ref _readSessionActive, 0);
        }
    }

    private sealed class ReadSessionLease(PostgreSqlTableCopyAdapter owner) : IDisposable
    {
        private PostgreSqlTableCopyAdapter? _owner = owner;
        public void Dispose()
        {
            PostgreSqlTableCopyAdapter? owner = Interlocked.Exchange(ref _owner, null);
            owner?.CloseReadSession();
        }
    }
}
