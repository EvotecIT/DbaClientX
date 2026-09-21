using DBAClientX.DataMovement;
using Oracle.ManagedDataAccess.Client;

namespace DBAClientX;

public sealed partial class OracleTableCopyAdapter : IDbaTableCopyDestinationCompatibilitySource
{
    internal const string OracleTableCopyNumericColumnsQuery = @"
SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE
FROM ALL_TAB_COLUMNS
WHERE OWNER = :owner
  AND TABLE_NAME = :table
  AND (DATA_TYPE IN ('NUMBER', 'FLOAT', 'BFILE', 'DATE', 'BINARY_FLOAT', 'BINARY_DOUBLE') OR DATA_TYPE LIKE 'TIMESTAMP%' OR DATA_TYPE LIKE 'INTERVAL DAY%' OR DATA_TYPE LIKE 'INTERVAL YEAR%')";

    internal const string OracleCompatibilityObjectQuery = @"SELECT 1
FROM ALL_OBJECTS
WHERE OWNER = :owner
  AND OBJECT_NAME = :name
  AND OBJECT_TYPE IN ('TABLE', 'VIEW', 'MATERIALIZED VIEW')
  AND ROWNUM = 1";

    internal const string OracleCompatibilitySynonymQuery = @"SELECT OWNER, TABLE_OWNER, TABLE_NAME, DB_LINK
FROM (
    SELECT OWNER, TABLE_OWNER, TABLE_NAME, DB_LINK,
           CASE WHEN OWNER = :owner THEN 0 ELSE 1 END AS OWNER_PRIORITY
    FROM ALL_SYNONYMS
    WHERE SYNONYM_NAME = :name
      AND (OWNER = :owner OR (:include_public = 1 AND OWNER = 'PUBLIC'))
    ORDER BY OWNER_PRIORITY
)
WHERE ROWNUM = 1";

    /// <inheritdoc />
    public async Task ValidateDestinationCompatibilityAsync(
        DbaTableCopyProvider destinationProvider,
        IReadOnlyList<DbaTableCopyDefinition> definitions,
        CancellationToken cancellationToken)
    {
        using OracleConnection? owned = _readConnection == null ? new OracleConnection(ConnectionString) : null;
        OracleConnection connection = _readConnection ?? owned!;
        if (owned != null) await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        string? currentOwner = null;
        foreach (DbaTableCopyDefinition definition in definitions)
        {
            IReadOnlyList<string> segments = DbaIdentifierPath.SplitSegments(
                definition.SourceName,
                DbaTableCopyProvider.Oracle);
            if (segments.Count is < 1 or > 2)
            {
                throw new ArgumentException(
                    "Oracle source compatibility validation requires a table name with an optional owner.",
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
                        Transaction = _readTransaction,
                        CommandTimeout = CommandTimeout
                    };
                    currentOwner = Convert.ToString(await currentSchema.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
                    if (string.IsNullOrWhiteSpace(currentOwner))
                        throw new InvalidOperationException("Oracle current schema could not be resolved for source compatibility validation.");
                }
                owner = currentOwner;
            }

            string table = Normalize(segments[segments.Count - 1]);
            (owner, table) = await ResolveCompatibilitySourceAsync(
                connection,
                owner,
                table,
                includePublic: segments.Count == 1,
                cancellationToken).ConfigureAwait(false);
            var regionColumns = new List<string>();
            var bcDateColumns = new List<string>();
            var binaryFloatColumns = new List<string>();
            using var command = new OracleCommand(OracleTableCopyNumericColumnsQuery, connection)
            {
                Transaction = _readTransaction,
                BindByName = true,
                CommandTimeout = CommandTimeout
            };
            command.Parameters.Add("owner", OracleDbType.Varchar2).Value = owner;
            command.Parameters.Add("table", OracleDbType.Varchar2).Value = table;
            using (OracleDataReader reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    string column = reader.GetString(0);
                    string dataType = reader.GetString(1);
                    int? precision = reader.IsDBNull(2) ? null : Convert.ToInt32(reader.GetValue(2));
                    int? scale = reader.IsDBNull(3) ? null : Convert.ToInt32(reader.GetValue(3));
                    bool excluded = IsExcludedProjection(definition, column) && !IsPagingColumn(definition, column);
                    if (string.Equals(dataType, "BFILE", StringComparison.OrdinalIgnoreCase))
                    {
                        ValidateOracleBFileProjection(definition.SourceName, column, excluded);
                        continue;
                    }
                    if (IsOracleTimestamp(dataType))
                    {
                        if (!excluded) ValidateOracleTimestampPrecision(column, dataType, scale);
                        if (!excluded && IsOracleTimestampWithTimeZone(dataType)) regionColumns.Add(column);
                        continue;
                    }
                    if (IsOracleDaySecondInterval(dataType))
                    {
                        if (!excluded) ValidateOracleDaySecondIntervalShape(column, dataType, precision, scale);
                        continue;
                    }
                    if (IsOracleYearMonthInterval(dataType))
                    {
                        ValidateOracleYearMonthProjection(
                            definition.SourceName,
                            column,
                            dataType,
                            destinationProvider,
                            excluded || IsPortableNumericProjection(definition, column),
                            precision);
                        continue;
                    }

                    if (string.Equals(dataType, "DATE", StringComparison.OrdinalIgnoreCase))
                    {
                        if (!excluded) bcDateColumns.Add(column);
                        continue;
                    }

                    if (IsOracleBinaryFloat(dataType))
                    {
                        if (!excluded &&
                            destinationProvider is not (DbaTableCopyProvider.Oracle or DbaTableCopyProvider.PostgreSql) &&
                            !IsPortableNumericProjection(definition, column))
                        {
                            binaryFloatColumns.Add(column);
                        }
                        continue;
                    }

                    if (destinationProvider is DbaTableCopyProvider.Oracle or DbaTableCopyProvider.MySql) continue;
                    if (IsPortableOracleNumeric(dataType, precision, scale) || IsPortableNumericProjection(definition, column)) continue;

                    string shape = dataType == "FLOAT"
                        ? precision.HasValue ? $"FLOAT({precision.Value})" : "unconstrained FLOAT"
                        : precision.HasValue && scale.HasValue
                            ? $"NUMBER({precision.Value},{scale.Value})"
                            : "unconstrained NUMBER";
                    throw new NotSupportedException(
                        $"Oracle source column '{definition.SourceName}.{column}' uses {shape}, which can exceed System.Decimal and is not portable to {destinationProvider}. " +
                        "Exclude the column, convert it explicitly to String, or copy it to an Oracle or MySQL destination.");
                }
            }

            await ValidateNoTimeZoneRegionsAsync(
                connection,
                definition.SourceName,
                regionColumns,
                cancellationToken).ConfigureAwait(false);
            await ValidateNoBcDatesAsync(
                connection,
                definition.SourceName,
                bcDateColumns,
                cancellationToken).ConfigureAwait(false);
            await ValidateNoBinaryFloatSpecialValuesAsync(
                connection,
                definition.SourceName,
                binaryFloatColumns,
                destinationProvider,
                cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task<(string Owner, string Table)> ResolveCompatibilitySourceAsync(
        OracleConnection connection,
        string owner,
        string table,
        bool includePublic,
        CancellationToken cancellationToken)
    {
        var visited = new HashSet<string>(StringComparer.Ordinal);
        for (var depth = 0; depth < 32; depth++)
        {
            if (!visited.Add(owner + "\0" + table))
            {
                throw new InvalidOperationException(
                    $"Oracle source synonym resolution for '{owner}.{table}' contains a cycle.");
            }

            using (var exists = new OracleCommand(OracleCompatibilityObjectQuery, connection)
            {
                Transaction = _readTransaction,
                BindByName = true,
                CommandTimeout = CommandTimeout
            })
            {
                exists.Parameters.Add("owner", OracleDbType.Varchar2).Value = owner;
                exists.Parameters.Add("name", OracleDbType.Varchar2).Value = table;
                if (await exists.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) != null)
                    return (owner, table);
            }

            using var synonym = new OracleCommand(OracleCompatibilitySynonymQuery, connection)
            {
                Transaction = _readTransaction,
                BindByName = true,
                CommandTimeout = CommandTimeout
            };
            synonym.Parameters.Add("owner", OracleDbType.Varchar2).Value = owner;
            synonym.Parameters.Add("name", OracleDbType.Varchar2).Value = table;
            synonym.Parameters.Add("include_public", OracleDbType.Int32).Value = includePublic ? 1 : 0;
            using OracleDataReader reader = await synonym.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false)) return (owner, table);

            string synonymOwner = reader.GetString(0);
            string targetOwner = reader.GetString(1);
            string targetTable = reader.GetString(2);
            string? databaseLink = reader.IsDBNull(3) ? null : reader.GetString(3);
            if (!string.IsNullOrWhiteSpace(databaseLink))
            {
                throw new NotSupportedException(
                    $"Oracle source synonym '{synonymOwner}.{table}' resolves through database link '{databaseLink}', whose remote column metadata cannot be validated before table-copy writes. Use a local view with explicit portable projections.");
            }

            owner = targetOwner;
            table = targetTable;
            includePublic = false;
        }

        throw new InvalidOperationException(
            "Oracle source synonym resolution exceeded the supported depth of 32 aliases.");
    }

    internal static void ValidateOracleBFileProjection(string sourceName, string columnName, bool excluded)
    {
        if (excluded) return;
        throw new NotSupportedException(
            $"Oracle source column '{sourceName}.{columnName}' uses BFILE, whose external file locator cannot be materialized or copied losslessly. " +
            "Exclude the column from the table-copy projection.");
    }

    internal static bool IsPortableOracleNumber(int? precision, int? scale)
    {
        if (precision is not > 0 || scale == null) return false;
        long effectivePrecision = precision.Value + Math.Max(0L, -(long)scale.Value);
        return precision <= 28 && scale <= 28 && effectivePrecision <= 28;
    }

    internal static bool IsPortableOracleNumeric(string dataType, int? precision, int? scale)
        => string.Equals(dataType, "NUMBER", StringComparison.OrdinalIgnoreCase) &&
           IsPortableOracleNumber(precision, scale);

    internal static void ValidateOracleTimestampPrecision(
        string columnName,
        string dataType,
        int? fractionalSecondPrecision)
    {
        if (!IsOracleTimestamp(dataType)) return;
        int effectivePrecision = fractionalSecondPrecision ?? 6;
        if (effectivePrecision <= 7) return;

        throw new NotSupportedException(
            $"Oracle table-copy column '{columnName}' uses data type '{dataType}' with fractional-second precision {effectivePrecision}, which exceeds the seven digits representable by CLR DateTime values. " +
            "Exclude the column or project it to a lossless text representation before copying.");
    }

    private static bool IsOracleTimestamp(string dataType)
        => dataType.StartsWith("TIMESTAMP", StringComparison.OrdinalIgnoreCase);

    private static bool IsOracleTimestampWithTimeZone(string dataType)
        => IsOracleTimestamp(dataType) &&
           dataType.IndexOf("WITH TIME ZONE", StringComparison.OrdinalIgnoreCase) >= 0;

    private static bool IsOracleDaySecondInterval(string dataType)
        => dataType.StartsWith("INTERVAL DAY", StringComparison.OrdinalIgnoreCase);

    private static bool IsOracleYearMonthInterval(string dataType)
        => dataType.StartsWith("INTERVAL YEAR", StringComparison.OrdinalIgnoreCase);

    private static bool IsOracleBinaryFloat(string dataType)
        => string.Equals(dataType, "BINARY_FLOAT", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(dataType, "BINARY_DOUBLE", StringComparison.OrdinalIgnoreCase);

    internal static void ValidateOracleYearMonthProjection(
        string sourceName,
        string columnName,
        string dataType,
        DbaTableCopyProvider destinationProvider,
        bool portableProjection,
        int? leadingYearPrecision = null)
    {
        if (!IsOracleYearMonthInterval(dataType) || portableProjection ||
            destinationProvider == DbaTableCopyProvider.Oracle)
        {
            return;
        }

        if (destinationProvider == DbaTableCopyProvider.PostgreSql)
        {
            int effectivePrecision = ResolveOracleYearLeadingPrecision(dataType, leadingYearPrecision);
            if (effectivePrecision <= 8) return;

            throw new NotSupportedException(
                $"Oracle source column '{sourceName}.{columnName}' uses {dataType} with leading year precision {effectivePrecision}, which can exceed PostgreSQL's 32-bit interval month range. " +
                "Exclude the column or convert it explicitly to String before copying to PostgreSQL.");
        }

        throw new NotSupportedException(
            $"Oracle source column '{sourceName}.{columnName}' uses {dataType}, whose calendar-month semantics are not portable to {destinationProvider}. " +
            "Exclude the column, convert it explicitly to String, or copy it to an Oracle or PostgreSQL destination.");
    }

    internal static int ResolveOracleYearLeadingPrecision(string dataType, int? metadataPrecision)
    {
        if (metadataPrecision.HasValue) return metadataPrecision.Value;
        int openParenthesis = dataType.IndexOf('(');
        if (openParenthesis >= 0)
        {
            int closeParenthesis = dataType.IndexOf(')', openParenthesis + 1);
            if (closeParenthesis > openParenthesis + 1 &&
                int.TryParse(
                    dataType.Substring(openParenthesis + 1, closeParenthesis - openParenthesis - 1),
                    System.Globalization.NumberStyles.None,
                    System.Globalization.CultureInfo.InvariantCulture,
                    out int parsed))
            {
                return parsed;
            }
        }

        return 2;
    }

    internal static void ValidateOracleDaySecondIntervalShape(
        string columnName,
        string dataType,
        int? dayPrecision,
        int? fractionalSecondPrecision)
    {
        if (!IsOracleDaySecondInterval(dataType)) return;
        int effectiveDayPrecision = dayPrecision ?? 2;
        int effectiveFractionalPrecision = fractionalSecondPrecision ?? 6;
        if (effectiveDayPrecision <= 7 && effectiveFractionalPrecision <= 7) return;

        throw new NotSupportedException(
            $"Oracle table-copy column '{columnName}' uses data type '{dataType}' with day precision {effectiveDayPrecision} and fractional-second precision {effectiveFractionalPrecision}, which exceeds the lossless CLR TimeSpan range or resolution. " +
            "Exclude the column or project it to a lossless text representation before copying.");
    }

    private async Task ValidateNoTimeZoneRegionsAsync(
        OracleConnection connection,
        string sourceName,
        IReadOnlyList<string> columns,
        CancellationToken cancellationToken)
    {
        if (columns.Count == 0) return;

        string predicates = string.Join(
            " OR ",
            columns.Select(static column =>
                $"SUBSTR(TO_CHAR({QuoteExactOracleIdentifier(column)}, 'TZR'), 1, 1) NOT IN ('+', '-')"));
        using var command = new OracleCommand(
            $"SELECT 1 FROM {QuotePath(sourceName)} WHERE ({predicates}) AND ROWNUM = 1",
            connection)
        {
            Transaction = _readTransaction,
            BindByName = true,
            CommandTimeout = CommandTimeout
        };
        if (await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) != null)
        {
            throw new NotSupportedException(
                $"Oracle source '{sourceName}' contains TIMESTAMP WITH TIME ZONE values backed by named regions, which cannot be represented losslessly by table-copy CLR DateTimeOffset values. " +
                "Exclude the affected column or project it to a fixed-offset or lossless text representation before copying.");
        }
    }

    private async Task ValidateNoBcDatesAsync(
        OracleConnection connection,
        string sourceName,
        IReadOnlyList<string> columns,
        CancellationToken cancellationToken)
    {
        if (columns.Count == 0) return;

        string predicates = string.Join(
            " OR ",
            columns.Select(static column =>
                $"TO_CHAR({QuoteExactOracleIdentifier(column)}, 'BC', 'NLS_DATE_LANGUAGE=English') = 'BC'"));
        using var command = new OracleCommand(
            $"SELECT 1 FROM {QuotePath(sourceName)} WHERE ({predicates}) AND ROWNUM = 1",
            connection)
        {
            Transaction = _readTransaction,
            BindByName = true,
            CommandTimeout = CommandTimeout
        };
        if (await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) != null)
        {
            throw new NotSupportedException(
                $"Oracle source '{sourceName}' contains BC DATE values that cannot be represented by CLR DateTime values. " +
                "Exclude the affected column or project it to a lossless text representation before copying.");
        }
    }

    private async Task ValidateNoBinaryFloatSpecialValuesAsync(
        OracleConnection connection,
        string sourceName,
        IReadOnlyList<string> columns,
        DbaTableCopyProvider destinationProvider,
        CancellationToken cancellationToken)
    {
        if (columns.Count == 0) return;

        string predicates = string.Join(
            " OR ",
            columns.Select(static column =>
            {
                string identifier = QuoteExactOracleIdentifier(column);
                return $"({identifier} IS NAN OR {identifier} IS INFINITE)";
            }));
        using var command = new OracleCommand(
            $"SELECT 1 FROM {QuotePath(sourceName)} WHERE ({predicates}) AND ROWNUM = 1",
            connection)
        {
            Transaction = _readTransaction,
            BindByName = true,
            CommandTimeout = CommandTimeout
        };
        if (await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) != null)
        {
            throw new NotSupportedException(
                $"Oracle source '{sourceName}' contains binary floating-point NaN or infinity values that cannot be copied losslessly to {destinationProvider}. " +
                "Exclude the affected column, convert it explicitly to String, or copy it to Oracle or PostgreSQL.");
        }
    }

    private static string QuoteExactOracleIdentifier(string identifier)
        => "\"" + identifier.Replace("\"", "\"\"") + "\"";

    internal static bool IsPortableNumericProjection(
        DbaTableCopyDefinition definition,
        string sourceColumn,
        bool allowStringConversion = true)
    {
        if (IsExcludedProjection(definition, sourceColumn) && !IsPagingColumn(definition, sourceColumn)) return true;

        if (!allowStringConversion || definition.ColumnTypeConversions == null) return false;
        string destinationColumn = ResolveDestinationColumn(definition, sourceColumn);
        IEqualityComparer<string> conversionComparer = definition.ColumnTypeConversions is Dictionary<string, DbaTableCopyColumnType> conversionDictionary
            ? conversionDictionary.Comparer
            : StringComparer.Ordinal;
        return definition.ColumnTypeConversions.Any(pair =>
            (conversionComparer.Equals(pair.Key, sourceColumn) ||
             conversionComparer.Equals(pair.Key, destinationColumn)) &&
            pair.Value == DbaTableCopyColumnType.String);
    }

    internal static bool ShouldMaterializeSourceColumn(DbaTableCopyDefinition definition, string sourceColumn)
        => !IsExcludedProjection(definition, sourceColumn) || IsPagingColumn(definition, sourceColumn);

    private static bool IsExcludedProjection(DbaTableCopyDefinition definition, string sourceColumn)
    {
        string destinationColumn = ResolveDestinationColumn(definition, sourceColumn);
        IEqualityComparer<string> excludedComparer = definition.ExcludedColumns is HashSet<string> excludedSet
            ? excludedSet.Comparer
            : StringComparer.Ordinal;
        return definition.ExcludedColumns?.Any(name =>
            excludedComparer.Equals(name, sourceColumn) ||
            excludedComparer.Equals(name, destinationColumn)) == true;
    }

    private static string ResolveDestinationColumn(DbaTableCopyDefinition definition, string sourceColumn)
    {
        IEqualityComparer<string> mappingComparer = definition.ColumnMappings is Dictionary<string, string> mappingDictionary
            ? mappingDictionary.Comparer
            : StringComparer.Ordinal;
        return definition.ColumnMappings?
            .FirstOrDefault(pair => mappingComparer.Equals(pair.Key, sourceColumn)).Value
            ?? sourceColumn;
    }

    private static bool IsPagingColumn(DbaTableCopyDefinition definition, string sourceColumn)
    {
        if (definition.OrderByColumns == null) return false;
        foreach (string planned in definition.OrderByColumns)
        {
            bool delimited = DbaIdentifierPath.IsDelimitedSegment(planned);
            string physical = DbaIdentifierPath.UnquoteSegment(planned, DbaTableCopyProvider.Oracle);
            if (!delimited) physical = physical.ToUpperInvariant();
            if (string.Equals(physical, sourceColumn, StringComparison.Ordinal)) return true;
        }

        return false;
    }
}
