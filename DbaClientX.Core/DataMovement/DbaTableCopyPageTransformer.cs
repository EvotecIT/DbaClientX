using System.Data;
using System.Globalization;

namespace DBAClientX.DataMovement;

/// <summary>
/// Applies provider-neutral page shaping before copied rows are written to a destination.
/// </summary>
internal static class DbaTableCopyPageTransformer
{
    internal static DataTable Transform(DataTable page, DbaTableCopyDefinition definition)
    {
        if (!HasTransforms(definition))
        {
            return page;
        }

        var excluded = new HashSet<string>(
            definition.ExcludedColumns ?? Array.Empty<string>(),
            StringComparer.Ordinal);
        var mappings = definition.ColumnMappings is { Count: > 0 }
            ? ToDictionary(definition.ColumnMappings)
            : null;
        var conversions = definition.ColumnTypeConversions is { Count: > 0 }
            ? ToDictionary(definition.ColumnTypeConversions)
            : null;

        var transformed = new DataTable(page.TableName);
        var columns = new List<ColumnTransform>();
        var destinationNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (DataColumn sourceColumn in page.Columns)
        {
            var destinationName = mappings != null && mappings.TryGetValue(sourceColumn.ColumnName, out var mappedName)
                ? mappedName
                : sourceColumn.ColumnName;
            if (excluded.Contains(sourceColumn.ColumnName) || excluded.Contains(destinationName))
            {
                continue;
            }

            var conversion = ResolveConversion(conversions, sourceColumn.ColumnName, destinationName);
            if (!destinationNames.Add(destinationName))
            {
                throw new InvalidOperationException(
                    $"Column mapping for source column '{sourceColumn.ColumnName}' produces duplicate destination column '{destinationName}'. Exclude the passthrough source column or choose a unique destination column name.");
            }

            AddDestinationColumn(transformed, sourceColumn, destinationName, conversion);
            columns.Add(new ColumnTransform(sourceColumn.ColumnName, destinationName, conversion));
        }

        foreach (DataRow sourceRow in page.Rows)
        {
            var destinationRow = transformed.NewRow();
            foreach (var column in columns)
            {
                destinationRow[column.DestinationName] = ConvertValue(sourceRow[column.SourceName], column.Conversion);
            }

            transformed.Rows.Add(destinationRow);
        }

        return transformed;
    }

    private static bool HasTransforms(DbaTableCopyDefinition definition)
        => definition.ColumnMappings is { Count: > 0 } ||
           definition.ExcludedColumns is { Count: > 0 } ||
           definition.ColumnTypeConversions is { Count: > 0 };

    private static DbaTableCopyColumnType ResolveConversion(
        Dictionary<string, DbaTableCopyColumnType>? conversions,
        string sourceName,
        string destinationName)
    {
        if (conversions == null)
        {
            return DbaTableCopyColumnType.None;
        }

        if (conversions.TryGetValue(sourceName, out var sourceConversion))
        {
            return sourceConversion;
        }

        return conversions.TryGetValue(destinationName, out var destinationConversion)
            ? destinationConversion
            : DbaTableCopyColumnType.None;
    }

    private static Dictionary<string, TValue> ToDictionary<TValue>(IReadOnlyDictionary<string, TValue> source)
    {
        var result = new Dictionary<string, TValue>(StringComparer.Ordinal);
        foreach (var entry in source)
        {
            result[entry.Key] = entry.Value;
        }

        return result;
    }

    private static void AddDestinationColumn(
        DataTable table,
        DataColumn sourceColumn,
        string destinationName,
        DbaTableCopyColumnType conversion)
    {
        if (conversion == DbaTableCopyColumnType.None)
        {
            var passthroughColumn = table.Columns.Add(destinationName, sourceColumn.DataType);
            passthroughColumn.AllowDBNull = sourceColumn.AllowDBNull;
            return;
        }

        var destinationColumn = conversion switch
        {
            DbaTableCopyColumnType.Boolean => table.Columns.Add(destinationName, typeof(bool)),
            DbaTableCopyColumnType.Int32 => table.Columns.Add(destinationName, typeof(int)),
            DbaTableCopyColumnType.Int64 => table.Columns.Add(destinationName, typeof(long)),
            DbaTableCopyColumnType.Decimal => table.Columns.Add(destinationName, typeof(decimal)),
            DbaTableCopyColumnType.String => table.Columns.Add(destinationName, typeof(string)),
            DbaTableCopyColumnType.DateTime => table.Columns.Add(destinationName, typeof(DateTime)),
            _ => table.Columns.Add(destinationName, typeof(object))
        };
        destinationColumn.AllowDBNull = sourceColumn.AllowDBNull;
    }

    private static object ConvertValue(object? value, DbaTableCopyColumnType conversion)
    {
        if (value == null || value == DBNull.Value || conversion == DbaTableCopyColumnType.None)
        {
            return value ?? DBNull.Value;
        }

        return conversion switch
        {
            DbaTableCopyColumnType.Boolean => ConvertToBoolean(value),
            DbaTableCopyColumnType.Int32 => Convert.ToInt32(value, CultureInfo.InvariantCulture),
            DbaTableCopyColumnType.Int64 => Convert.ToInt64(value, CultureInfo.InvariantCulture),
            DbaTableCopyColumnType.Decimal => Convert.ToDecimal(value, CultureInfo.InvariantCulture),
            DbaTableCopyColumnType.String => Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty,
            DbaTableCopyColumnType.DateTime => Convert.ToDateTime(value, CultureInfo.InvariantCulture),
            _ => value
        };
    }

    private static bool ConvertToBoolean(object value)
    {
        if (value is bool boolean)
        {
            return boolean;
        }

        if (bool.TryParse(value.ToString(), out var parsed))
        {
            return parsed;
        }

        return Convert.ToInt64(value, CultureInfo.InvariantCulture) != 0;
    }

    private sealed record ColumnTransform(
        string SourceName,
        string DestinationName,
        DbaTableCopyColumnType Conversion);
}
