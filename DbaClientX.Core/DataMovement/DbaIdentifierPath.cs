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
                return string.Equals(trimmed, trimmed.ToUpperInvariant(), StringComparison.Ordinal) &&
                       !IsReservedIdentifier(trimmed, DbaTableCopyProvider.Oracle)
                    ? trimmed
                    : "\"" + trimmed.Replace("\"", "\"\"") + "\"";
            }

            if (provider == DbaTableCopyProvider.PostgreSql)
            {
                return string.Equals(trimmed, trimmed.ToLowerInvariant(), StringComparison.Ordinal) &&
                       !IsReservedIdentifier(trimmed, DbaTableCopyProvider.PostgreSql)
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

    public static bool IsReservedIdentifier(string identifier, DbaTableCopyProvider provider)
        => provider switch
        {
            DbaTableCopyProvider.Oracle => OracleReservedWords.Contains(identifier),
            DbaTableCopyProvider.PostgreSql => PostgreSqlReservedWords.Contains(identifier),
            _ => false
        };

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

    private static readonly HashSet<string> OracleReservedWords = new(StringComparer.OrdinalIgnoreCase)
    {
        "ACCESS", "ADD", "ALL", "ALTER", "AND", "ANY", "AS", "ASC", "AUDIT", "BETWEEN", "BY", "CHAR", "CHECK",
        "CLUSTER", "COLUMN", "COMMENT", "COMPRESS", "CONNECT", "CREATE", "CURRENT", "DATE", "DECIMAL", "DEFAULT",
        "DELETE", "DESC", "DISTINCT", "DROP", "ELSE", "EXCLUSIVE", "EXISTS", "FILE", "FLOAT", "FOR", "FROM",
        "GRANT", "GROUP", "HAVING", "IDENTIFIED", "IMMEDIATE", "IN", "INCREMENT", "INDEX", "INITIAL", "INSERT",
        "INTEGER", "INTERSECT", "INTO", "IS", "LEVEL", "LIKE", "LOCK", "LONG", "MAXEXTENTS", "MINUS", "MLSLABEL",
        "MODE", "MODIFY", "NOAUDIT", "NOCOMPRESS", "NOT", "NOWAIT", "NULL", "NUMBER", "OF", "OFFLINE", "ON",
        "ONLINE", "OPTION", "OR", "ORDER", "PCTFREE", "PRIOR", "PRIVILEGES", "PUBLIC", "RAW", "RENAME",
        "RESOURCE", "REVOKE", "ROW", "ROWID", "ROWNUM", "ROWS", "SELECT", "SESSION", "SET", "SHARE", "SIZE",
        "SMALLINT", "START", "SUCCESSFUL", "SYNONYM", "SYSDATE", "TABLE", "THEN", "TO", "TRIGGER", "UID",
        "UNION", "UNIQUE", "UPDATE", "USER", "VALIDATE", "VALUES", "VARCHAR", "VARCHAR2", "VIEW", "WHENEVER",
        "WHERE", "WITH"
    };

    private static readonly HashSet<string> PostgreSqlReservedWords = new(StringComparer.OrdinalIgnoreCase)
    {
        "ALL", "ANALYSE", "ANALYZE", "AND", "ANY", "ARRAY", "AS", "ASC", "ASYMMETRIC", "AUTHORIZATION",
        "BINARY", "BOTH", "CASE", "CAST", "CHECK", "COLLATE", "COLLATION", "COLUMN", "CONCURRENTLY",
        "CONSTRAINT", "CREATE", "CROSS", "CURRENT_CATALOG", "CURRENT_DATE", "CURRENT_ROLE", "CURRENT_SCHEMA",
        "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "DEFAULT", "DEFERRABLE", "DESC", "DISTINCT", "DO",
        "ELSE", "END", "EXCEPT", "FALSE", "FETCH", "FOR", "FOREIGN", "FREEZE", "FROM", "FULL", "GRANT",
        "GROUP", "HAVING", "ILIKE", "IN", "INITIALLY", "INNER", "INTERSECT", "INTO", "IS", "ISNULL", "JOIN",
        "LATERAL", "LEADING", "LEFT", "LIKE", "LIMIT", "LOCALTIME", "LOCALTIMESTAMP", "NATURAL", "NOT",
        "NOTNULL", "NULL", "OFFSET", "ON", "ONLY", "OR", "ORDER", "OUTER", "OVERLAPS", "PLACING", "PRIMARY",
        "REFERENCES", "RETURNING", "RIGHT", "SELECT", "SESSION_USER", "SIMILAR", "SOME", "SYMMETRIC", "TABLE",
        "TABLESAMPLE", "THEN", "TO", "TRAILING", "TRUE", "UNION", "UNIQUE", "USER", "USING", "VARIADIC",
        "VERBOSE", "WHEN", "WHERE", "WINDOW", "WITH"
    };
}
