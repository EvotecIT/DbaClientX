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
  AND DATA_TYPE IN ('NUMBER', 'FLOAT')";

    /// <inheritdoc />
    public async Task ValidateDestinationCompatibilityAsync(
        DbaTableCopyProvider destinationProvider,
        IReadOnlyList<DbaTableCopyDefinition> definitions,
        CancellationToken cancellationToken)
    {
        if (destinationProvider is DbaTableCopyProvider.Oracle or DbaTableCopyProvider.MySql) return;

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
            using var command = new OracleCommand(OracleTableCopyNumericColumnsQuery, connection)
            {
                Transaction = _readTransaction,
                BindByName = true,
                CommandTimeout = CommandTimeout
            };
            command.Parameters.Add("owner", OracleDbType.Varchar2).Value = owner;
            command.Parameters.Add("table", OracleDbType.Varchar2).Value = table;
            using OracleDataReader reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                string column = reader.GetString(0);
                string dataType = reader.GetString(1);
                int? precision = reader.IsDBNull(2) ? null : Convert.ToInt32(reader.GetValue(2));
                int? scale = reader.IsDBNull(3) ? null : Convert.ToInt32(reader.GetValue(3));
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

    internal static bool IsPortableNumericProjection(DbaTableCopyDefinition definition, string sourceColumn)
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
