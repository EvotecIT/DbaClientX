using DBAClientX.DataMovement;
using Npgsql;

namespace DBAClientX;

public sealed partial class PostgreSqlTableCopyAdapter : IDbaTableCopyDestinationCompatibilitySource
{
    internal const string PostgreSqlProviderSpecificColumnsQuery = @"
SELECT attribute.attname,
       value_type.typname,
       value_type.typtype::text,
       element_type.typname,
       element_type.typtype::text
FROM pg_catalog.pg_attribute AS attribute
JOIN pg_catalog.pg_class AS relation ON relation.oid = attribute.attrelid
JOIN pg_catalog.pg_type AS value_type ON value_type.oid = attribute.atttypid
LEFT JOIN pg_catalog.pg_type AS element_type ON element_type.oid = value_type.typelem
WHERE relation.oid = to_regclass(@name)
  AND attribute.attnum > 0
  AND NOT attribute.attisdropped
ORDER BY attribute.attnum";

    private static readonly HashSet<string> ProviderSpecificTypeNames = new(StringComparer.Ordinal)
    {
        "point", "line", "lseg", "box", "path", "polygon", "circle",
        "tsquery", "tsvector", "pg_lsn", "tid", "bit", "varbit", "hstore"
    };

    /// <inheritdoc />
    public async Task ValidateDestinationCompatibilityAsync(
        DbaTableCopyProvider destinationProvider,
        IReadOnlyList<DbaTableCopyDefinition> definitions,
        CancellationToken cancellationToken)
    {
        if (destinationProvider == DbaTableCopyProvider.PostgreSql) return;

        using NpgsqlConnection? owned = _readConnection == null ? new NpgsqlConnection(ConnectionString) : null;
        NpgsqlConnection connection = _readConnection ?? owned!;
        if (owned != null) await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        foreach (DbaTableCopyDefinition definition in definitions)
        {
            IReadOnlyList<string> segments = DbaIdentifierPath.SplitSegments(
                definition.SourceName,
                DbaTableCopyProvider.PostgreSql);
            if (segments.Count is < 1 or > 2)
            {
                throw new ArgumentException(
                    "PostgreSQL source compatibility validation requires a table name with an optional schema.",
                    nameof(definitions));
            }

            using var command = new NpgsqlCommand(PostgreSqlProviderSpecificColumnsQuery, connection, _readTransaction)
            {
                CommandTimeout = CommandTimeout
            };
            command.Parameters.AddWithValue("@name", QuotePath(definition.SourceName));
            using NpgsqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                string column = reader.GetString(0);
                string typeName = reader.GetString(1);
                string typeKind = reader.GetString(2);
                string? elementTypeName = reader.IsDBNull(3) ? null : reader.GetString(3);
                string? elementTypeKind = reader.IsDBNull(4) ? null : reader.GetString(4);
                if (!IsProviderSpecificPostgreSqlType(typeName, typeKind, elementTypeName, elementTypeKind) ||
                    IsPortableProviderProjection(definition, column))
                {
                    continue;
                }

                throw new NotSupportedException(
                    $"PostgreSQL source column '{definition.SourceName}.{column}' uses provider-specific type '{typeName}', which is not portable to {destinationProvider}. " +
                    "Exclude the column, convert it explicitly to String, or copy it to a PostgreSQL destination.");
            }
        }
    }

    internal static bool IsProviderSpecificPostgreSqlType(
        string typeName,
        string typeKind,
        string? elementTypeName,
        string? elementTypeKind)
        => IsProviderSpecificPostgreSqlScalar(typeName, typeKind) ||
           (elementTypeName != null && IsProviderSpecificPostgreSqlScalar(elementTypeName, elementTypeKind));

    private static bool IsProviderSpecificPostgreSqlScalar(string typeName, string? typeKind)
        => typeKind is "r" or "m" or "c" || ProviderSpecificTypeNames.Contains(typeName);

    internal static bool IsPortableProviderProjection(DbaTableCopyDefinition definition, string sourceColumn)
    {
        IEqualityComparer<string> mappingComparer = definition.ColumnMappings is Dictionary<string, string> mappingDictionary
            ? mappingDictionary.Comparer
            : StringComparer.Ordinal;
        string destinationColumn = definition.ColumnMappings?
            .FirstOrDefault(pair => mappingComparer.Equals(pair.Key, sourceColumn)).Value
            ?? sourceColumn;

        IEqualityComparer<string> excludedComparer = definition.ExcludedColumns is HashSet<string> excludedSet
            ? excludedSet.Comparer
            : StringComparer.Ordinal;
        if (definition.ExcludedColumns?.Any(name =>
                excludedComparer.Equals(name, sourceColumn) ||
                excludedComparer.Equals(name, destinationColumn)) == true)
        {
            return true;
        }

        if (definition.ColumnTypeConversions == null) return false;
        IEqualityComparer<string> conversionComparer = definition.ColumnTypeConversions is Dictionary<string, DbaTableCopyColumnType> conversionDictionary
            ? conversionDictionary.Comparer
            : StringComparer.Ordinal;
        return definition.ColumnTypeConversions.Any(pair =>
            (conversionComparer.Equals(pair.Key, sourceColumn) ||
             conversionComparer.Equals(pair.Key, destinationColumn)) &&
            pair.Value == DbaTableCopyColumnType.String);
    }
}
