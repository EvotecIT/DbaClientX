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
  AND (DATA_TYPE IN ('NUMBER', 'FLOAT', 'BFILE') OR DATA_TYPE LIKE 'TIMESTAMP%' OR DATA_TYPE LIKE 'INTERVAL DAY%' OR DATA_TYPE LIKE 'INTERVAL YEAR%')";

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
            var regionColumns = new List<string>();
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
                            excluded || IsPortableNumericProjection(definition, column));
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
        }
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

    internal static void ValidateOracleYearMonthProjection(
        string sourceName,
        string columnName,
        string dataType,
        DbaTableCopyProvider destinationProvider,
        bool portableProjection)
    {
        if (!IsOracleYearMonthInterval(dataType) || portableProjection ||
            destinationProvider is DbaTableCopyProvider.Oracle or DbaTableCopyProvider.PostgreSql)
        {
            return;
        }

        throw new NotSupportedException(
            $"Oracle source column '{sourceName}.{columnName}' uses {dataType}, whose calendar-month semantics are not portable to {destinationProvider}. " +
            "Exclude the column, convert it explicitly to String, or copy it to an Oracle or PostgreSQL destination.");
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
