using System.Data;
using System.Globalization;

namespace DBAClientX.DataMovement;

/// <summary>
/// Applies provider-neutral page shaping before copied rows are written to a destination.
/// </summary>
internal static class DbaTableCopyPageTransformer
{
    internal static DataTable TransformReadback(DataTable page, DbaTableCopyDefinition definition)
    {
        if (definition.ColumnTypeConversions is not { Count: > 0 }) return Transform(page, definition);
        // Bind the effective source conversions to the same physical columns the content
        // hasher reads. Providers such as SQLite may return different identifier casing.
        var conversions = definition.ColumnTypeConversions.ToDictionary(
            pair => page.Columns[pair.Key]?.ColumnName ?? throw new InvalidOperationException($"Copied column '{pair.Key}' is missing from the verification result."),
            pair => pair.Value, StringComparer.Ordinal);
        return Transform(page, definition with { ColumnTypeConversions = conversions });
    }

    internal static DataTable Transform(DataTable page, DbaTableCopyDefinition definition)
    {
        if (!HasTransforms(definition))
        {
            return page;
        }

        var projection = new ColumnProjection(definition);
        var transformed = new DataTable(page.TableName);
        var columns = new List<ColumnTransform>();
        var destinationNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (DataColumn sourceColumn in page.Columns)
        {
            var destinationName = projection.Map(sourceColumn.ColumnName);
            if (projection.IsExcluded(sourceColumn.ColumnName, destinationName))
            {
                continue;
            }

            var conversion = projection.Conversion(sourceColumn.ColumnName, destinationName);
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

    internal static DestinationReadProjection ResolveDestinationReadProjection(DataTable source, DbaTableCopyDefinition definition)
    {
        var projection = new ColumnProjection(definition);
        IReadOnlyList<string> keys = definition.DestinationOrderByColumns ?? definition.OrderByColumns!.Select(key =>
        {
            string sourceName = source.Columns[key]?.ColumnName ?? key;
            string destinationName = projection.Map(sourceName);
            if (projection.IsExcluded(sourceName, destinationName))
                throw new ArgumentException("Content-verified copies require the ordered key to be copied unchanged. Preserve identity keys or select another unique key.");
            return destinationName;
        }).ToArray();
        var conversions = new Dictionary<string, DbaTableCopyColumnType>(StringComparer.Ordinal);
        foreach (DataColumn column in source.Columns)
        {
            string destinationName = projection.Map(column.ColumnName);
            if (projection.IsExcluded(column.ColumnName, destinationName)) continue;
            DbaTableCopyColumnType conversion = projection.Conversion(column.ColumnName, destinationName);
            if (conversion != DbaTableCopyColumnType.None) conversions[destinationName] = conversion;
        }
        return new DestinationReadProjection(keys, conversions);
    }

    internal sealed record DestinationReadProjection(IReadOnlyList<string> Keys, IReadOnlyDictionary<string, DbaTableCopyColumnType> Conversions);

    private sealed class ColumnProjection
    {
        private readonly HashSet<string> _excluded;
        private readonly Dictionary<string, string>? _mappings;
        private readonly Dictionary<string, DbaTableCopyColumnType>? _conversions;

        internal ColumnProjection(DbaTableCopyDefinition definition)
        {
            _excluded = definition.ExcludedColumns == null ? new HashSet<string>(StringComparer.Ordinal) : ToHashSet(definition.ExcludedColumns);
            _mappings = definition.ColumnMappings == null ? null : ToDictionary(definition.ColumnMappings);
            _conversions = definition.ColumnTypeConversions == null ? null : ToDictionary(definition.ColumnTypeConversions);
        }

        internal string Map(string sourceName) => _mappings != null && _mappings.TryGetValue(sourceName, out string? mapped) ? mapped : sourceName;
        internal bool IsExcluded(string sourceName, string destinationName) => _excluded.Contains(sourceName) || _excluded.Contains(destinationName);
        internal DbaTableCopyColumnType Conversion(string sourceName, string destinationName) => ResolveConversion(_conversions, sourceName, destinationName);
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
        var result = new Dictionary<string, TValue>(GetComparer(source));
        foreach (var entry in source)
        {
            result[entry.Key] = entry.Value;
        }

        return result;
    }

    private static HashSet<string> ToHashSet(IReadOnlyCollection<string> source)
    {
        var result = new HashSet<string>(GetComparer(source));
        foreach (var value in source)
        {
            result.Add(value);
        }

        return result;
    }

    private static IEqualityComparer<string> GetComparer<TValue>(IReadOnlyDictionary<string, TValue> source)
        => source is Dictionary<string, TValue> dictionary
            ? dictionary.Comparer
            : StringComparer.Ordinal;

    private static IEqualityComparer<string> GetComparer(IReadOnlyCollection<string> source)
        => source is HashSet<string> hashSet
            ? hashSet.Comparer
            : StringComparer.Ordinal;

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
