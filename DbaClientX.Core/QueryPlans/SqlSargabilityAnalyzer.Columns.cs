using System;
using System.Collections.Generic;

namespace DBAClientX.QueryPlans;

public static partial class SqlSargabilityAnalyzer
{
    /// <summary>The first token of a column reference ending at <paramref name="column"/> (<c>t.Name</c> starts at <c>t</c>).</summary>
    private static int QualifiedStart(IReadOnlyList<SqlToken> tokens, int column)
    {
        while (column > 1 && tokens[column - 1].Text == "." && IsColumnReference(tokens, column - 2))
        {
            column -= 2;
        }

        return column;
    }

    /// <summary>Whether the tokens from <paramref name="index"/> start with a column (an identifier, possibly qualified), not a value.</summary>
    private static bool IsColumnReference(IReadOnlyList<SqlToken> tokens, int index)
    {
        var token = tokens[index];
        if (token.Kind == SqlTokenKind.QuotedIdentifier)
        {
            return true;
        }

        if (token.Kind != SqlTokenKind.Word || IsWord(token, "SELECT") || IsWord(token, "NULL") || IsWord(token, "DISTINCT") ||
            IsWord(token, "CURRENT_TIMESTAMP") || IsWord(token, "CURRENT_DATE") || IsWord(token, "CURRENT_TIME"))
        {
            return false;
        }

        // A word followed by '(' is a function call: its own arguments are checked when the scan reaches it.
        return index + 1 >= tokens.Count || tokens[index + 1].Kind != SqlTokenKind.OpenParenthesis;
    }

    /// <summary>Functions whose first argument is a date part or type name, not a value.</summary>
    private static bool SkipsFirstArgument(string function)
        => function.Equals("DATEADD", StringComparison.OrdinalIgnoreCase) || function.Equals("DATEDIFF", StringComparison.OrdinalIgnoreCase) ||
           function.Equals("DATEPART", StringComparison.OrdinalIgnoreCase) || function.Equals("DATENAME", StringComparison.OrdinalIgnoreCase) ||
           function.Equals("DATETRUNC", StringComparison.OrdinalIgnoreCase) || function.Equals("EXTRACT", StringComparison.OrdinalIgnoreCase);

    // Type names that CONVERT takes as an argument (SQL Server CONVERT(type, value), MySQL CONVERT(value, type)).
    private static readonly HashSet<string> TypeNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "DATE", "TIME", "DATETIME", "DATETIME2", "DATETIMEOFFSET", "SMALLDATETIME", "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT",
        "BIT", "DECIMAL", "NUMERIC", "FLOAT", "REAL", "DOUBLE", "MONEY", "CHAR", "NCHAR", "VARCHAR", "NVARCHAR", "TEXT", "NTEXT",
        "BINARY", "VARBINARY", "UNIQUEIDENTIFIER", "SIGNED", "UNSIGNED", "JSON", "XML"
    };

    // Words inside an expression that are not columns.
    private static readonly HashSet<string> OperandKeywords = new(StringComparer.OrdinalIgnoreCase)
    {
        "AS", "AND", "OR", "NOT", "IS", "NULL", "IN", "BETWEEN", "LIKE", "ILIKE", "GLOB", "REGEXP", "MATCH", "ESCAPE", "CASE",
        "WHEN", "THEN", "ELSE", "END", "FROM", "USING", "DISTINCT", "ALL", "TRUE", "FALSE", "CURRENT_TIMESTAMP", "CURRENT_DATE",
        "CURRENT_TIME", "BOTH", "LEADING", "TRAILING", "INTERVAL", "COLLATE", "SELECT", "EXISTS"
    };

    // Words that continue a type after its first word (double precision, character varying, timestamp with time zone).
    private static readonly HashSet<string> TypeContinuations = new(StringComparer.OrdinalIgnoreCase)
    {
        "PRECISION", "VARYING", "WITH", "WITHOUT", "TIME", "ZONE", "LOCAL"
    };

    /// <summary>The index after the type that starts at <paramref name="index"/> (after <c>::</c>): its words, length and <c>[]</c>.</summary>
    private static int SkipCastType(IReadOnlyList<SqlToken> tokens, int index)
    {
        if (index < tokens.Count && (tokens[index].Kind is SqlTokenKind.Word or SqlTokenKind.QuotedIdentifier))
        {
            index++;
        }

        while (index < tokens.Count)
        {
            if (tokens[index].Kind == SqlTokenKind.Word && TypeContinuations.Contains(tokens[index].Text))
            {
                index++;
            }
            else if (tokens[index].Kind == SqlTokenKind.OpenParenthesis)
            {
                var close = FindClosingParenthesis(tokens, index);
                index = close < 0 ? tokens.Count : close + 1;
            }
            else if (tokens[index].Text is "[" or "]" or "[]")
            {
                index++;
            }
            else
            {
                break;
            }
        }

        return index;
    }

    private static readonly HashSet<string> IntervalUnits = new(StringComparer.OrdinalIgnoreCase)
    {
        "YEAR", "YEARS", "QUARTER", "MONTH", "MONTHS", "WEEK", "WEEKS", "DAY", "DAYS", "HOUR", "HOURS", "MINUTE", "MINUTES",
        "SECOND", "SECONDS", "MILLISECOND", "MICROSECOND"
    };

    /// <summary>The index of the <c>,</c> or <c>)</c> that ends the argument holding <paramref name="index"/>.</summary>
    private static int SkipToArgumentEnd(IReadOnlyList<SqlToken> tokens, int index)
    {
        var depth = 0;
        for (; index < tokens.Count; index++)
        {
            if (tokens[index].Kind == SqlTokenKind.OpenParenthesis)
            {
                depth++;
            }
            else if (tokens[index].Kind == SqlTokenKind.CloseParenthesis)
            {
                if (depth == 0)
                {
                    return index;
                }

                depth--;
            }
            else if (depth == 0 && tokens[index].Text == ",")
            {
                return index;
            }
        }

        return index;
    }

    /// <summary>
    /// The first column inside the call or parentheses opened at <paramref name="open"/>, at any depth (inside nested
    /// calls such as <c>dbx_lower(NULLIF(Name, ''))</c> too, but not inside a subquery), as the index of its last name
    /// part; -1 when the arguments hold no column. Type names after <c>AS</c>, collation names, keywords and the date
    /// part a function takes first (<c>DATEADD(day, …)</c>, <c>EXTRACT(YEAR FROM …)</c>) are not columns.
    /// </summary>
    private static int FindColumnInCall(IReadOnlyList<SqlToken> tokens, int open, bool skipFirstArgument)
    {
        var argument = 0;
        var afterFrom = false;
        for (var index = open + 1; index < tokens.Count; index++)
        {
            var token = tokens[index];
            var skipped = skipFirstArgument && argument == 0;
            var next = index + 1 < tokens.Count ? tokens[index + 1] : default;
            if (token.Kind == SqlTokenKind.CloseParenthesis)
            {
                return -1;
            }

            if (token.Kind == SqlTokenKind.Symbol && token.Text == ",")
            {
                argument++;
                continue;
            }

            if (token.Kind == SqlTokenKind.OpenParenthesis)
            {
                var subquery = index + 1 < tokens.Count && (IsWord(tokens[index + 1], "SELECT") || IsWord(tokens[index + 1], "WITH") || IsWord(tokens[index + 1], "VALUES"));
                if (!skipped && !subquery && FindColumnInCall(tokens, index, false) is var inner and >= 0)
                {
                    return inner;
                }

                index = FindClosingParenthesis(tokens, index);
                if (index < 0)
                {
                    return -1;
                }

                continue;
            }

            if (IsWord(token, "FROM") || (afterFrom && IsWord(token, "FOR")))
            {
                // EXTRACT(part FROM value), SUBSTRING(value FROM n FOR m): what follows is another argument.
                afterFrom = true;
                argument++;
                continue;
            }

            if (token.Kind == SqlTokenKind.Word && next.Kind == SqlTokenKind.OpenParenthesis)
            {
                // A call. Not ours: a subquery after a keyword (EXISTS (SELECT …), IN (SELECT …)), a type's length
                // (CONVERT(NVARCHAR(MAX), x)), a window or filter clause (OVER (…), FILTER (…)), ANY/SOME (…).
                var subquery = index + 2 < tokens.Count && (IsWord(tokens[index + 2], "SELECT") || IsWord(tokens[index + 2], "WITH") || IsWord(tokens[index + 2], "VALUES"));
                var foreign = subquery || TypeNames.Contains(token.Text) || IsWord(token, "OVER") || IsWord(token, "FILTER") ||
                              IsWord(token, "ANY") || IsWord(token, "SOME");
                if (!skipped && !foreign && FindColumnInCall(tokens, index + 1, SkipsFirstArgument(token.Text)) is var nested and >= 0)
                {
                    return nested;
                }

                index = FindClosingParenthesis(tokens, index + 1);
                if (index < 0)
                {
                    return -1;
                }

                continue;
            }

            if (IsWord(token, "AS") || IsWord(token, "USING"))
            {
                // CAST(x AS DOUBLE PRECISION), CAST(x AS NVARCHAR(MAX)), CONVERT(x USING utf8mb4): a type or character
                // set follows, up to the end of the argument.
                index = SkipToArgumentEnd(tokens, index + 1) - 1;
                continue;
            }

            if (token.Text == "::")
            {
                // x::timestamptz, x::numeric(10, 2), x::text[], x::double precision: only the type.
                index = SkipCastType(tokens, index + 1) - 1;
                continue;
            }

            if (IsWord(token, "AT") && IsWord(next, "TIME") && index + 2 < tokens.Count && IsWord(tokens[index + 2], "ZONE"))
            {
                index += 2;
                continue;
            }

            if ((IsWord(token, "x") || IsWord(token, "X")) && next.Kind == SqlTokenKind.String && next.Position == token.Position + 1)
            {
                // A blob literal, x'00'.
                index++;
                continue;
            }

            if (IsWord(token, "COLLATE"))
            {
                // A collation name, possibly qualified (pg_catalog."C").
                index++;
                while (index + 2 < tokens.Count && tokens[index + 1].Text == ".")
                {
                    index += 2;
                }

                continue;
            }

            if (IsWord(token, "INTERVAL"))
            {
                // INTERVAL 7 DAY, INTERVAL '1 day': a literal value and maybe a unit. A column or expression as the value
                // is read as usual.
                if (next.Kind is SqlTokenKind.Number or SqlTokenKind.String)
                {
                    index++;
                    if (index + 1 < tokens.Count && tokens[index + 1].Kind == SqlTokenKind.Word && IntervalUnits.Contains(tokens[index + 1].Text))
                    {
                        index++;
                    }
                }

                continue;
            }

            if (skipped || !IsOperandColumn(token))
            {
                continue;
            }

            while (index + 2 < tokens.Count && tokens[index + 1].Text == "." && (tokens[index + 2].Kind is SqlTokenKind.Word or SqlTokenKind.QuotedIdentifier))
            {
                index += 2;
            }

            return index;
        }

        return -1;
    }

    /// <summary>Whether a token inside an expression names a column: an identifier that is not a keyword or a type name.</summary>
    private static bool IsOperandColumn(SqlToken token)
        => token.Kind == SqlTokenKind.QuotedIdentifier ||
           (token.Kind == SqlTokenKind.Word && !OperandKeywords.Contains(token.Text) && !TypeNames.Contains(token.Text));
}
