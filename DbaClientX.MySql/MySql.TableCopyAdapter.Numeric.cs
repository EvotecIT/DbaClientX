using System.Data;
using System.Globalization;
using DBAClientX.DataMovement;
using MySqlConnector;

namespace DBAClientX;

public sealed partial class MySqlTableCopyAdapter
{
    /// <inheritdoc />
    public async Task ValidateDestinationCompatibilityAsync(
        DbaTableCopyProvider destinationProvider,
        IReadOnlyList<DbaTableCopyDefinition> definitions,
        CancellationToken cancellationToken)
    {
        if (destinationProvider is DbaTableCopyProvider.MySql or DbaTableCopyProvider.Oracle) return;

        await using MySqlConnection? owned = _readConnection == null
            ? new MySqlConnection(ResolveMySqlRegularOperationConnectionString())
            : null;
        MySqlConnection connection = _readConnection ?? owned!;
        if (owned != null) await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        foreach (DbaTableCopyDefinition definition in definitions)
        {
            string[] segments = DbaIdentifierPath.SplitSegments(definition.SourceName, DbaTableCopyProvider.MySql)
                .Select(segment => DbaIdentifierPath.UnquoteSegment(segment, DbaTableCopyProvider.MySql))
                .ToArray();
            if (segments.Length is < 1 or > 2)
            {
                throw new ArgumentException(
                    "MySQL source compatibility validation requires a table name with an optional database.",
                    nameof(definitions));
            }

            string database = segments.Length == 2 ? segments[0] : connection.Database;
            string table = segments[segments.Length - 1];
            await using var command = new MySqlCommand(
                @"SELECT COLUMN_NAME, NUMERIC_PRECISION
FROM INFORMATION_SCHEMA.COLUMNS
WHERE ((@@lower_case_table_names = 0 AND BINARY TABLE_SCHEMA = BINARY @database AND BINARY TABLE_NAME = BINARY @table)
       OR (@@lower_case_table_names <> 0 AND TABLE_SCHEMA = @database AND TABLE_NAME = @table))
  AND DATA_TYPE IN ('decimal', 'numeric')",
                connection,
                _readTransaction)
            {
                CommandTimeout = CommandTimeout
            };
            command.Parameters.AddWithValue("@database", database);
            command.Parameters.AddWithValue("@table", table);
            await using MySqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                string column = reader.GetString(0);
                int precision = reader.GetInt32(1);
                if (precision <= 28 || IsPortableDecimalProjection(definition, column)) continue;
                throw new NotSupportedException(
                    $"MySQL source column '{definition.SourceName}.{column}' uses DECIMAL precision {precision}, which can exceed System.Decimal and is not portable to {destinationProvider}. " +
                    "Exclude the column, convert it explicitly to String, or copy it to a MySQL or Oracle destination.");
            }
        }
    }

    internal static bool IsPortableDecimalProjection(DbaTableCopyDefinition definition, string sourceColumn)
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

    internal static object ReadProviderValue(MySqlDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal)) return DBNull.Value;
        return IsMySqlDecimal(reader.GetDataTypeName(ordinal))
            ? NormalizeMySqlDecimal(reader.GetMySqlDecimal(ordinal))
            : reader.GetValue(ordinal);
    }

    internal static Type GetNormalizedFieldType(Type providerType, string dataTypeName)
        => IsMySqlDecimal(dataTypeName) ? typeof(object) : providerType;

    internal static object NormalizeMySqlDecimal(MySqlDecimal number)
    {
        string text = number.ToString();
        return decimal.TryParse(
            text,
            NumberStyles.AllowLeadingSign | NumberStyles.AllowDecimalPoint,
            CultureInfo.InvariantCulture,
            out decimal value)
            ? value
            : new DbaArbitraryDecimal(text);
    }

    internal static void AddPageParameter(MySqlCommand command, string name, object? value)
    {
        if (value is DbaArbitraryDecimal number)
        {
            var parameter = command.Parameters.Add(name, MySqlDbType.NewDecimal);
            parameter.Value = number.CanonicalValue;
            return;
        }

        command.Parameters.AddWithValue(name, value ?? DBNull.Value);
    }

    internal static DataTable? NormalizeBulkPage(DataTable page)
    {
        var arbitraryDecimalColumns = page.Columns.Cast<DataColumn>()
            .Select(static column => column.DataType == typeof(DbaArbitraryDecimal))
            .ToArray();
        foreach (DataRow row in page.Rows)
        {
            for (var index = 0; index < page.Columns.Count; index++)
            {
                if (row[index] is DbaArbitraryDecimal) arbitraryDecimalColumns[index] = true;
            }
        }
        if (!arbitraryDecimalColumns.Any(static value => value)) return null;

        var normalized = new DataTable { CaseSensitive = page.CaseSensitive };
        for (var index = 0; index < page.Columns.Count; index++)
        {
            DataColumn sourceColumn = page.Columns[index];
            Type destinationType = arbitraryDecimalColumns[index]
                ? sourceColumn.DataType == typeof(DbaArbitraryDecimal) ? typeof(string) : typeof(object)
                : sourceColumn.DataType;
            DataColumn destinationColumn = normalized.Columns.Add(sourceColumn.ColumnName, destinationType);
            destinationColumn.AllowDBNull = sourceColumn.AllowDBNull;
            if (destinationType == typeof(DateTime)) destinationColumn.DateTimeMode = sourceColumn.DateTimeMode;
            if (destinationType == typeof(string) && sourceColumn.DataType == typeof(string))
                destinationColumn.MaxLength = sourceColumn.MaxLength;
        }

        foreach (DataRow row in page.Rows)
        {
            object?[] values = row.ItemArray;
            for (var index = 0; index < values.Length; index++)
            {
                if (values[index] is DbaArbitraryDecimal number) values[index] = number.CanonicalValue;
            }
            normalized.Rows.Add(values);
        }
        return normalized;
    }

    internal static string CreateTableIdentity(string database, string table)
    {
        if (database == null) throw new ArgumentNullException(nameof(database));
        if (table == null) throw new ArgumentNullException(nameof(table));
        return database.Length.ToString(CultureInfo.InvariantCulture) + ":" + database +
               table.Length.ToString(CultureInfo.InvariantCulture) + ":" + table;
    }

    private static bool IsMySqlDecimal(string dataTypeName)
    {
        string normalized = dataTypeName.Trim().ToUpperInvariant();
        return normalized is "DECIMAL" or "NEWDECIMAL" or "NUMERIC";
    }
}
