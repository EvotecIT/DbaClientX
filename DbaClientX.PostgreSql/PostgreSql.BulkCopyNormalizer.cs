using System.Data;
#if NET8_0_OR_GREATER
using System.Diagnostics.CodeAnalysis;
#endif
using NpgsqlTypes;

namespace DBAClientX.DataMovement;

/// <summary>
/// Normalizes PostgreSQL bulk-copy destination names to match PostgreSQL unquoted identifier folding before provider COPY quoting.
/// </summary>
public static class DbaPostgreSqlBulkCopyNormalizer
{
    /// <summary>
    /// Normalizes an ordinary PostgreSQL destination table path while preserving explicitly quoted segments.
    /// </summary>
    public static string NormalizeDestinationTableName(string destinationTableName)
        => string.Join(
            ".",
            SplitIdentifierPath(destinationTableName).Select(static segment =>
            {
                if (segment.IsExplicitlyQuoted)
                {
                    return "\"" + segment.Value.Replace("\"", "\"\"") + "\"";
                }

                return IsPostgreSqlSimpleIdentifier(segment.Value)
                    ? segment.Value.ToLowerInvariant()
                    : segment.Value;
            }));

    /// <summary>
    /// Returns the original page when no column changes are needed, otherwise returns a copied table with normalized column names.
    /// </summary>
    public static DataTable NormalizePage(DataTable page, string destinationTableName)
    {
        if (page == null)
        {
            throw new ArgumentNullException(nameof(page));
        }

        var normalizedNames = page.Columns
            .Cast<DataColumn>()
            .Select(static column => NormalizeColumnName(column.ColumnName))
            .ToArray();
        var networkColumns = page.Columns.Cast<DataColumn>()
            .Select(static column => column.DataType == typeof(DbaIpNetwork))
            .ToArray();
        var intervalColumns = page.Columns.Cast<DataColumn>()
            .Select(static column => column.DataType == typeof(DbaYearMonthInterval) || column.DataType == typeof(DbaCalendarInterval))
            .ToArray();
        var dateTimeOffsetColumns = page.Columns.Cast<DataColumn>()
            .Select(static column => column.DataType == typeof(DateTimeOffset))
            .ToArray();
        bool normalizeDateTimeOffsets = false;
        foreach (DataRow row in page.Rows)
        {
            for (var index = 0; index < page.Columns.Count; index++)
            {
                if (row[index] is DbaIpNetwork) networkColumns[index] = true;
                if (row[index] is DbaYearMonthInterval or DbaCalendarInterval) intervalColumns[index] = true;
                if (row[index] is DateTimeOffset instant)
                {
                    dateTimeOffsetColumns[index] = true;
                    if (instant.Offset != TimeSpan.Zero) normalizeDateTimeOffsets = true;
                }
            }
        }
        bool normalizeNames = !page.Columns.Cast<DataColumn>()
            .Select(static column => column.ColumnName)
            .SequenceEqual(normalizedNames, StringComparer.Ordinal);
        bool normalizeNetworks = networkColumns.Any(static value => value);
        bool normalizeIntervals = intervalColumns.Any(static value => value);
        if (!normalizeNames && !normalizeNetworks && !normalizeIntervals && !normalizeDateTimeOffsets)
        {
            return page;
        }

        var duplicates = normalizedNames
            .GroupBy(static name => name, StringComparer.Ordinal)
            .FirstOrDefault(static group => group.Count() > 1);
        if (duplicates != null)
        {
            throw new InvalidOperationException($"PostgreSQL bulk copy column normalization would create duplicate destination column '{duplicates.Key}'.");
        }

        if (!normalizeNetworks && !normalizeIntervals)
        {
            var renamed = page.Copy();
            for (var index = 0; index < renamed.Columns.Count; index++)
                renamed.Columns[index].ColumnName = normalizedNames[index];
            NormalizeDateTimeOffsets(renamed, dateTimeOffsetColumns);
            return renamed;
        }

        foreach (DataRow row in page.Rows)
        {
            for (var index = 0; index < page.Columns.Count; index++)
            {
                object value = row[index];
                if (value == DBNull.Value) continue;
                if (networkColumns[index] && value is not DbaIpNetwork && !IsProviderNetwork(value))
                {
                    throw new InvalidOperationException(
                        $"PostgreSQL network column '{page.Columns[index].ColumnName}' contains an incompatible value of type '{value.GetType().FullName}'.");
                }
                if (intervalColumns[index] && value is not DbaYearMonthInterval and not DbaCalendarInterval and not NpgsqlInterval)
                {
                    throw new InvalidOperationException(
                        $"PostgreSQL year-month interval column '{page.Columns[index].ColumnName}' contains an incompatible value of type '{value.GetType().FullName}'.");
                }
            }
        }

        var normalized = new DataTable { CaseSensitive = page.CaseSensitive };
        for (var index = 0; index < page.Columns.Count; index++)
        {
            DataColumn sourceColumn = page.Columns[index];
            DataColumn destinationColumn = normalized.Columns.Add(
                normalizedNames[index],
                networkColumns[index]
                    ? GetProviderNetworkType()
                    : intervalColumns[index]
                        ? typeof(NpgsqlInterval)
                        : sourceColumn.DataType);
            destinationColumn.AllowDBNull = sourceColumn.AllowDBNull;
            if (destinationColumn.DataType == typeof(DateTime))
                destinationColumn.DateTimeMode = sourceColumn.DateTimeMode;
            if (destinationColumn.DataType == typeof(string))
                destinationColumn.MaxLength = sourceColumn.MaxLength;
        }
        foreach (DataRow row in page.Rows)
        {
            object?[] values = row.ItemArray;
            for (var index = 0; index < values.Length; index++)
            {
                if (values[index] is DbaIpNetwork network) values[index] = CreateProviderNetwork(network);
                if (values[index] is DbaYearMonthInterval interval)
                    values[index] = CreateProviderYearMonthInterval(interval, page.Columns[index].ColumnName);
                if (values[index] is DbaCalendarInterval calendarInterval)
                    values[index] = new NpgsqlInterval(calendarInterval.Months, calendarInterval.Days, calendarInterval.Microseconds);
                if (values[index] is DateTimeOffset instant && dateTimeOffsetColumns[index])
                    values[index] = instant.ToUniversalTime();
            }
            normalized.Rows.Add(values);
        }
        return normalized;
    }

    private static void NormalizeDateTimeOffsets(DataTable page, IReadOnlyList<bool> dateTimeOffsetColumns)
    {
        foreach (DataRow row in page.Rows)
        {
            for (var index = 0; index < page.Columns.Count; index++)
            {
                if (dateTimeOffsetColumns[index] && row[index] is DateTimeOffset instant && instant.Offset != TimeSpan.Zero)
                    row[index] = instant.ToUniversalTime();
            }
        }
    }

#if NET8_0_OR_GREATER
    [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
#endif
    private static Type GetProviderNetworkType()
        => typeof(NpgsqlInet);

    private static bool IsProviderNetwork(object value)
        => value is NpgsqlInet
#if NET472
           or NpgsqlCidr
#else
           or System.Net.IPNetwork
#endif
        ;

    private static object CreateProviderNetwork(DbaIpNetwork network)
        => new NpgsqlInet(network.Address, checked((byte)network.PrefixLength));

    private static NpgsqlInterval CreateProviderYearMonthInterval(DbaYearMonthInterval interval, string columnName)
    {
        if (interval.TotalMonths is < int.MinValue or > int.MaxValue)
        {
            throw new InvalidOperationException(
                $"PostgreSQL year-month interval column '{columnName}' contains {interval.TotalMonths} months, which exceeds the provider's native interval range.");
        }

        return new NpgsqlInterval((int)interval.TotalMonths, 0, 0);
    }

    private static string NormalizeColumnName(string columnName)
    {
        var trimmed = columnName.Trim();
        if (trimmed.Length >= 2 && trimmed[0] == '"' && trimmed[trimmed.Length - 1] == '"')
        {
            return trimmed.Substring(1, trimmed.Length - 2).Replace("\"\"", "\"");
        }

        return IsPostgreSqlSimpleIdentifier(trimmed)
            ? trimmed.ToLowerInvariant()
            : trimmed;
    }

    private static IReadOnlyList<IdentifierSegment> SplitIdentifierPath(string identifierPath)
    {
        if (string.IsNullOrWhiteSpace(identifierPath))
        {
            throw new ArgumentException("Identifier cannot be null or whitespace.", nameof(identifierPath));
        }

        var parts = SplitIdentifierPathSegments(identifierPath);
        if (parts.Any(static part => string.IsNullOrWhiteSpace(part)))
        {
            throw new ArgumentException($"Identifier '{identifierPath}' contains an empty path segment.", nameof(identifierPath));
        }

        return parts.Select(NormalizeIdentifierSegment).ToArray();
    }

    private static IReadOnlyList<string> SplitIdentifierPathSegments(string identifierPath)
    {
        var parts = new List<string>();
        var start = 0;
        var quote = '\0';
        for (var index = 0; index < identifierPath.Length; index++)
        {
            var value = identifierPath[index];
            if (quote == '\0')
            {
                if (value is '"' or '[' or '`')
                {
                    quote = value;
                    continue;
                }

                if (value == '.')
                {
                    parts.Add(identifierPath.Substring(start, index - start));
                    start = index + 1;
                }

                continue;
            }

            if (quote == '"' && value == '"')
            {
                if (index + 1 < identifierPath.Length && identifierPath[index + 1] == '"')
                {
                    index++;
                    continue;
                }

                quote = '\0';
                continue;
            }

            if (quote == '[' && value == ']')
            {
                if (index + 1 < identifierPath.Length && identifierPath[index + 1] == ']')
                {
                    index++;
                    continue;
                }

                quote = '\0';
                continue;
            }

            if (quote == '`' && value == '`')
            {
                if (index + 1 < identifierPath.Length && identifierPath[index + 1] == '`')
                {
                    index++;
                    continue;
                }

                quote = '\0';
            }
        }

        if (quote != '\0')
        {
            throw new ArgumentException($"Identifier '{identifierPath}' contains an unterminated delimited path segment.", nameof(identifierPath));
        }

        parts.Add(identifierPath.Substring(start));
        return parts;
    }

    private static IdentifierSegment NormalizeIdentifierSegment(string identifier)
    {
        var trimmed = identifier.Trim();
        if (trimmed.Length >= 2 &&
            trimmed[0] == '"' &&
            trimmed[trimmed.Length - 1] == '"')
        {
            return new IdentifierSegment(trimmed.Substring(1, trimmed.Length - 2).Replace("\"\"", "\""), true);
        }

        return new IdentifierSegment(trimmed, false);
    }

    private static bool IsPostgreSqlSimpleIdentifier(string identifier)
    {
        if (identifier.Length == 0 || !IsPostgreSqlIdentifierStart(identifier[0]))
        {
            return false;
        }

        for (var i = 1; i < identifier.Length; i++)
        {
            if (!IsPostgreSqlIdentifierPart(identifier[i]))
            {
                return false;
            }
        }

        return true;
    }

    private static bool IsPostgreSqlIdentifierStart(char value)
        => value is >= 'A' and <= 'Z' or >= 'a' and <= 'z' or '_';

    private static bool IsPostgreSqlIdentifierPart(char value)
        => IsPostgreSqlIdentifierStart(value) || value is >= '0' and <= '9' or '$';

    private readonly struct IdentifierSegment
    {
        internal IdentifierSegment(string value, bool isExplicitlyQuoted)
        {
            Value = value;
            IsExplicitlyQuoted = isExplicitlyQuoted;
        }

        internal string Value { get; }

        internal bool IsExplicitlyQuoted { get; }
    }
}
