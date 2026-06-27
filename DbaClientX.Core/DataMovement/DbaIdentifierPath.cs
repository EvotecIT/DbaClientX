namespace DBAClientX.DataMovement;

internal static class DbaIdentifierPath
{
    public static string QuotePlanSegment(string segment)
    {
        var trimmed = segment.Trim();
        if (IsDelimitedSegment(trimmed) || IsSimplePlanSegment(trimmed))
        {
            return trimmed;
        }

        return "\"" + trimmed.Replace("\"", "\"\"") + "\"";
    }

    public static string QuotePlanSegmentPreservingCase(string segment)
        => QuotePlanSegmentPreservingCase(segment, provider: null);

    public static string QuotePlanSegmentPreservingCase(string segment, DbaTableCopyProvider? provider)
    {
        var trimmed = segment.Trim();
        if (IsDelimitedSegment(trimmed))
        {
            return trimmed;
        }

        if (IsSimplePlanSegment(trimmed))
        {
            if (provider == DbaTableCopyProvider.Oracle)
            {
                return string.Equals(trimmed, trimmed.ToUpperInvariant(), StringComparison.Ordinal)
                    ? trimmed
                    : "\"" + trimmed.Replace("\"", "\"\"") + "\"";
            }

            if (string.Equals(trimmed, trimmed.ToLowerInvariant(), StringComparison.Ordinal))
            {
                return trimmed;
            }
        }

        return "\"" + trimmed.Replace("\"", "\"\"") + "\"";
    }

    public static string UnquoteSegment(string segment)
    {
        var trimmed = segment.Trim();
        if (trimmed.Length >= 2 && trimmed[0] == '"' && trimmed[trimmed.Length - 1] == '"')
        {
            return trimmed.Substring(1, trimmed.Length - 2).Replace("\"\"", "\"");
        }

        if (trimmed.Length >= 2 && trimmed[0] == '[' && trimmed[trimmed.Length - 1] == ']')
        {
            return trimmed.Substring(1, trimmed.Length - 2).Replace("]]", "]");
        }

        if (trimmed.Length >= 2 && trimmed[0] == '`' && trimmed[trimmed.Length - 1] == '`')
        {
            return trimmed.Substring(1, trimmed.Length - 2).Replace("``", "`");
        }

        return trimmed;
    }

    public static IReadOnlyList<string> SplitSegments(string identifierPath)
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

    public static string NormalizeForDuplicateCheck(string identifierPath)
        => string.Join(".", SplitSegments(identifierPath).Select(UnquoteSegment));

    private static bool IsDelimitedSegment(string segment)
        => segment.Length >= 2 &&
           ((segment[0] == '"' && segment[segment.Length - 1] == '"') ||
            (segment[0] == '[' && segment[segment.Length - 1] == ']') ||
            (segment[0] == '`' && segment[segment.Length - 1] == '`'));

    private static bool IsSimplePlanSegment(string segment)
    {
        if (segment.Length == 0 || !IsSimplePlanIdentifierStart(segment[0]))
        {
            return false;
        }

        for (var index = 1; index < segment.Length; index++)
        {
            if (!IsSimplePlanIdentifierPart(segment[index]))
            {
                return false;
            }
        }

        return true;
    }

    private static bool IsSimplePlanIdentifierStart(char value)
        => value is >= 'A' and <= 'Z' or >= 'a' and <= 'z' or '_';

    private static bool IsSimplePlanIdentifierPart(char value)
        => IsSimplePlanIdentifierStart(value) || value is >= '0' and <= '9';
}
