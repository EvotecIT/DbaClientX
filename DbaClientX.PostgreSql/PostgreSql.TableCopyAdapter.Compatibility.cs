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
       element_type.typtype::text,
       pg_catalog.format_type(attribute.atttypid, attribute.atttypmod)
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
        "tsquery", "tsvector", "pg_lsn", "tid", "bit", "varbit", "hstore",
        "inet", "cidr", "macaddr", "macaddr8"
    };

    /// <inheritdoc />
    public async Task ValidateDestinationCompatibilityAsync(
        DbaTableCopyProvider destinationProvider,
        IReadOnlyList<DbaTableCopyDefinition> definitions,
        CancellationToken cancellationToken)
    {
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

            var infinityColumns = new List<(string Column, bool IsArray)>();
            var numericColumns = new List<(string Column, bool IsArray)>();
            using var command = new NpgsqlCommand(PostgreSqlProviderSpecificColumnsQuery, connection, _readTransaction)
            {
                CommandTimeout = CommandTimeout
            };
            command.Parameters.AddWithValue("@name", QuotePath(definition.SourceName));
            using (NpgsqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    string column = reader.GetString(0);
                    string typeName = reader.GetString(1);
                    string typeKind = reader.GetString(2);
                    string? elementTypeName = reader.IsDBNull(3) ? null : reader.GetString(3);
                    string? elementTypeKind = reader.IsDBNull(4) ? null : reader.GetString(4);
                    string formattedType = reader.GetString(5);
                    bool isArray = elementTypeName != null;
                    if ((IsPostgreSqlInfinityCapableType(typeName) ||
                         (elementTypeName != null && IsPostgreSqlInfinityCapableType(elementTypeName))) &&
                        !IsPortableProviderProjection(definition, column, allowStringConversion: false))
                    {
                        infinityColumns.Add((column, isArray));
                    }
                    if ((string.Equals(typeName, "numeric", StringComparison.Ordinal) ||
                         string.Equals(elementTypeName, "numeric", StringComparison.Ordinal)) &&
                        !IsPortableProviderProjection(definition, column, allowStringConversion: false))
                    {
                        if (isArray) ValidateNumericArrayShape(column, formattedType);
                        numericColumns.Add((column, isArray));
                    }

                    if (destinationProvider == DbaTableCopyProvider.PostgreSql ||
                        !IsProviderSpecificPostgreSqlType(typeName, typeKind, elementTypeName, elementTypeKind) ||
                        IsPortableProviderProjection(definition, column, allowStringConversion: !isArray))
                    {
                        continue;
                    }

                    throw new NotSupportedException(
                        $"PostgreSQL source column '{definition.SourceName}.{column}' uses provider-specific type '{typeName}', which is not portable to {destinationProvider}. " +
                        (isArray
                            ? "Exclude the column or copy it to a PostgreSQL destination. Array-to-String conversion is not lossless."
                            : "Exclude the column, convert it explicitly to String, or copy it to a PostgreSQL destination."));
                }
            }

            await ValidateNoInfinitySentinelsAsync(
                connection,
                definition.SourceName,
                infinityColumns,
                cancellationToken).ConfigureAwait(false);
            await ValidateNoNumericSpecialValuesAsync(
                connection,
                definition.SourceName,
                numericColumns,
                cancellationToken).ConfigureAwait(false);
        }
    }

    internal static bool IsProviderSpecificPostgreSqlType(
        string typeName,
        string typeKind,
        string? elementTypeName,
        string? elementTypeKind)
        => elementTypeName != null || IsProviderSpecificPostgreSqlScalar(typeName, typeKind);

    private static bool IsProviderSpecificPostgreSqlScalar(string typeName, string? typeKind)
        => typeKind is "r" or "m" or "c" || ProviderSpecificTypeNames.Contains(typeName);

    internal static bool IsPostgreSqlInfinityCapableType(string typeName)
        => typeName is "date" or "timestamp" or "timestamptz";

    private async Task ValidateNoInfinitySentinelsAsync(
        NpgsqlConnection connection,
        string sourceName,
        IReadOnlyList<(string Column, bool IsArray)> columns,
        CancellationToken cancellationToken)
    {
        if (columns.Count == 0) return;

        string predicates = string.Join(
            " OR ",
            columns.Select(static column =>
            {
                string identifier = QuoteExactPostgreSqlIdentifier(column.Column);
                return column.IsArray
                    ? $"EXISTS (SELECT 1 FROM unnest({identifier}) AS dbax_value WHERE dbax_value IN ('infinity', '-infinity'))"
                    : $"{identifier} IN ('infinity', '-infinity')";
            }));
        using var command = new NpgsqlCommand(
            $"SELECT EXISTS (SELECT 1 FROM {QuotePath(sourceName)} WHERE {predicates})",
            connection,
            _readTransaction)
        {
            CommandTimeout = CommandTimeout
        };
        if (Convert.ToBoolean(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false)))
        {
            throw new NotSupportedException(
                $"PostgreSQL source '{sourceName}' contains date or timestamp infinity sentinels that cannot be represented losslessly by table-copy CLR values. " +
                "Exclude the affected column or filter out the sentinel values before copying.");
        }
    }

    internal static void ValidateNumericArrayShape(string columnName, string formattedType)
    {
        const string suffix = "[]";
        string normalized = formattedType.Trim().ToLowerInvariant();
        if (!normalized.EndsWith(suffix, StringComparison.Ordinal))
        {
            throw new NotSupportedException(
                $"PostgreSQL table-copy column '{columnName}' reports unsupported numeric array declaration '{formattedType}'.");
        }

        string elementDeclaration = normalized.Substring(0, normalized.Length - suffix.Length);
        if (elementDeclaration == "numeric" || elementDeclaration == "decimal")
        {
            ValidateNumericShape(columnName, precision: null, scale: null);
            return;
        }

        int openParenthesis = elementDeclaration.IndexOf('(');
        int closeParenthesis = elementDeclaration.LastIndexOf(')');
        if (openParenthesis <= 0 || closeParenthesis != elementDeclaration.Length - 1)
        {
            throw new NotSupportedException(
                $"PostgreSQL table-copy column '{columnName}' reports unsupported numeric array declaration '{formattedType}'.");
        }

        string[] parts = elementDeclaration.Substring(openParenthesis + 1, closeParenthesis - openParenthesis - 1)
            .Split(',');
        int scale = 0;
        if (parts.Length is < 1 or > 2 ||
            !int.TryParse(parts[0].Trim(), out int precision) ||
            (parts.Length == 2 && !int.TryParse(parts[1].Trim(), out scale)))
        {
            throw new NotSupportedException(
                $"PostgreSQL table-copy column '{columnName}' reports unsupported numeric array declaration '{formattedType}'.");
        }

        ValidateNumericShape(columnName, precision, scale);
    }

    private async Task ValidateNoNumericSpecialValuesAsync(
        NpgsqlConnection connection,
        string sourceName,
        IReadOnlyList<(string Column, bool IsArray)> columns,
        CancellationToken cancellationToken)
    {
        if (columns.Count == 0) return;

        string predicates = string.Join(
            " OR ",
            columns.Select(static column =>
            {
                string identifier = QuoteExactPostgreSqlIdentifier(column.Column);
                return column.IsArray
                    ? $"EXISTS (SELECT 1 FROM unnest({identifier}) AS dbax_value WHERE dbax_value::text IN ('NaN', 'Infinity', '-Infinity'))"
                    : $"{identifier}::text IN ('NaN', 'Infinity', '-Infinity')";
            }));
        using var command = new NpgsqlCommand(
            $"SELECT EXISTS (SELECT 1 FROM {QuotePath(sourceName)} WHERE {predicates})",
            connection,
            _readTransaction)
        {
            CommandTimeout = CommandTimeout
        };
        if (Convert.ToBoolean(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false)))
        {
            throw new NotSupportedException(
                $"PostgreSQL source '{sourceName}' contains numeric NaN or infinity values that cannot be represented losslessly by table-copy CLR decimal values. " +
                "Exclude the affected column or filter out the special values before copying.");
        }
    }

    private static string QuoteExactPostgreSqlIdentifier(string identifier)
        => "\"" + identifier.Replace("\"", "\"\"") + "\"";

    internal static bool IsPortableProviderProjection(
        DbaTableCopyDefinition definition,
        string sourceColumn,
        bool allowStringConversion = true)
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

        if (!allowStringConversion || definition.ColumnTypeConversions == null) return false;
        IEqualityComparer<string> conversionComparer = definition.ColumnTypeConversions is Dictionary<string, DbaTableCopyColumnType> conversionDictionary
            ? conversionDictionary.Comparer
            : StringComparer.Ordinal;
        return definition.ColumnTypeConversions.Any(pair =>
            (conversionComparer.Equals(pair.Key, sourceColumn) ||
             conversionComparer.Equals(pair.Key, destinationColumn)) &&
            pair.Value == DbaTableCopyColumnType.String);
    }
}
