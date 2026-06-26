using System.Data;

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
        if (page.Columns.Cast<DataColumn>().Select(static column => column.ColumnName).SequenceEqual(normalizedNames, StringComparer.Ordinal))
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

        var normalized = page.Copy();
        for (var i = 0; i < normalized.Columns.Count; i++)
        {
            normalized.Columns[i].ColumnName = normalizedNames[i];
        }

        return normalized;
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
