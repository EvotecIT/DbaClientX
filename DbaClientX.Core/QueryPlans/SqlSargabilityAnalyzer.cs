using System;
using System.Collections.Generic;

namespace DBAClientX.QueryPlans;

/// <summary>
/// Finds predicates in SQL text that an index probably cannot serve: a function wrapped around a column in a
/// <c>WHERE</c> or <c>ON</c> condition, and <c>LIKE</c>/<c>GLOB</c> patterns that start with a wildcard.
/// </summary>
/// <remarks>
/// <para>This is a heuristic over tokens, not a parser and not a plan: it reports what is worth a look and can be
/// wrong both ways. An expression index on exactly <c>LOWER(Name)</c> makes <c>LOWER(Name) = @p</c> indexable, a
/// column of a small table needs no index, and a pattern passed as a parameter is not seen. Implicit conversions
/// (comparing a column with a value of another type) are not detected; explicit <c>CAST</c> and <c>CONVERT</c> are.
/// Confirm a finding with the database's plan (<c>SQLite.ExplainQueryPlanAsync</c> and <see cref="QueryPlanAssert"/>).</para>
/// </remarks>
public static class SqlSargabilityAnalyzer
{
    private static readonly HashSet<string> WrappingFunctions = new(StringComparer.OrdinalIgnoreCase)
    {
        "LOWER", "UPPER", "LCASE", "UCASE", "TRIM", "LTRIM", "RTRIM", "SUBSTR", "SUBSTRING", "LEFT", "RIGHT", "REPLACE",
        "CAST", "CONVERT", "TRY_CAST", "TRY_CONVERT", "COALESCE", "IFNULL", "ISNULL", "NVL", "NULLIF", "LENGTH", "LEN",
        "DATE", "TIME", "DATETIME", "JULIANDAY", "UNIXEPOCH", "STRFTIME", "YEAR", "MONTH", "DAY", "DATEPART", "DATEADD",
        "DATEDIFF", "DATE_TRUNC", "EXTRACT", "TO_CHAR", "TO_DATE", "ABS", "ROUND", "FLOOR", "CEILING", "CEIL", "HEX",
        "INSTR", "PRINTF", "FORMAT", "CONCAT", "UNICODE", "TYPEOF"
    };

    // Keywords that end a WHERE/ON condition at the same nesting level.
    private static readonly HashSet<string> ClauseKeywords = new(StringComparer.OrdinalIgnoreCase)
    {
        "SELECT", "FROM", "JOIN", "GROUP", "ORDER", "HAVING", "LIMIT", "OFFSET", "FETCH", "WINDOW", "UNION", "INTERSECT",
        "EXCEPT", "RETURNING", "SET", "VALUES"
    };

    /// <summary>Returns the predicates of <paramref name="sql"/> that an index probably cannot serve.</summary>
    /// <param name="sql">One or more SQL statements.</param>
    /// <returns>The findings in text order; empty when none are found.</returns>
    public static IReadOnlyList<SqlSargabilityFinding> Analyze(string sql)
    {
        if (sql == null)
        {
            throw new ArgumentNullException(nameof(sql));
        }

        var tokens = SqlTokenizer.Tokenize(sql);
        var findings = new List<SqlSargabilityFinding>();
        // Whether each open parenthesis level is inside a WHERE or ON condition.
        var inCondition = new Stack<bool>();
        inCondition.Push(false);
        // ON starts a condition only after JOIN at the same nesting level; CREATE INDEX ... ON, ON CONFLICT and
        // ON DELETE do not, and a derived table between JOIN and ON keeps the JOIN pending.
        var joinPending = new Stack<bool>();
        joinPending.Push(false);
        for (var index = 0; index < tokens.Count; index++)
        {
            var token = tokens[index];
            switch (token.Kind)
            {
                case SqlTokenKind.OpenParenthesis:
                    inCondition.Push(inCondition.Peek());
                    joinPending.Push(false);
                    continue;
                case SqlTokenKind.CloseParenthesis:
                    if (inCondition.Count > 1)
                    {
                        inCondition.Pop();
                        joinPending.Pop();
                    }

                    continue;
                case SqlTokenKind.Semicolon:
                    inCondition.Clear();
                    inCondition.Push(false);
                    joinPending.Clear();
                    joinPending.Push(false);
                    continue;
                case SqlTokenKind.Word when IsWord(token, "WHERE") || (IsWord(token, "ON") && joinPending.Peek()):
                    SetTop(joinPending, false);
                    SetCondition(inCondition, true);
                    continue;
                case SqlTokenKind.Word when IsWord(token, "ON"):
                    continue;
                case SqlTokenKind.Word when ClauseKeywords.Contains(token.Text):
                    SetTop(joinPending, IsWord(token, "JOIN"));
                    SetCondition(inCondition, false);
                    continue;
            }

            if (!inCondition.Peek())
            {
                continue;
            }

            if (token.Kind == SqlTokenKind.Word && WrappingFunctions.Contains(token.Text) &&
                index + 2 < tokens.Count && tokens[index + 1].Kind == SqlTokenKind.OpenParenthesis &&
                HasColumnArgument(tokens, index + 1, SkipsFirstArgument(token.Text)))
            {
                var end = FindClosingParenthesis(tokens, index + 1);
                var text = end < 0 ? sql.Substring(token.Position) : sql.Substring(token.Position, tokens[end].Position + 1 - token.Position);
                findings.Add(new SqlSargabilityFinding(
                    SqlSargabilityFindingKind.FunctionOnColumn,
                    token.Position,
                    text,
                    $"{token.Text.ToUpperInvariant()}() wraps a column in a condition, so an index on that column cannot serve it; compare the stored value, store a normalized column, or index the expression."));
            }
            else if (IsColumnReference(tokens, index) && index + 1 < tokens.Count && tokens[index + 1].Text == "::")
            {
                var end = Math.Min(index + 2, tokens.Count - 1);
                findings.Add(new SqlSargabilityFinding(
                    SqlSargabilityFindingKind.FunctionOnColumn,
                    token.Position,
                    sql.Substring(token.Position, tokens[end].Position + tokens[end].Text.Length - token.Position),
                    "A :: cast converts a column in a condition, so an index on that column cannot serve it; cast the compared value instead."));
            }
            else if (token.Kind == SqlTokenKind.Word && (IsWord(token, "LIKE") || IsWord(token, "ILIKE") || IsWord(token, "GLOB")) &&
                     index + 1 < tokens.Count && tokens[index + 1].Kind == SqlTokenKind.String &&
                     StartsWithWildcard(tokens[index + 1].Value, IsWord(token, "GLOB")))
            {
                var pattern = tokens[index + 1];
                findings.Add(new SqlSargabilityFinding(
                    SqlSargabilityFindingKind.LeadingWildcard,
                    token.Position,
                    sql.Substring(token.Position, pattern.Position + pattern.Text.Length - token.Position),
                    $"{token.Text.ToUpperInvariant()} pattern starts with a wildcard, so an index cannot narrow it and every row is read; use a prefix pattern or a full-text index."));
            }
        }

        return findings;
    }

    private static void SetCondition(Stack<bool> inCondition, bool value) => SetTop(inCondition, value);

    private static void SetTop(Stack<bool> stack, bool value)
    {
        stack.Pop();
        stack.Push(value);
    }

    private static bool IsWord(SqlToken token, string word)
        => token.Kind == SqlTokenKind.Word && string.Equals(token.Text, word, StringComparison.OrdinalIgnoreCase);

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

    /// <summary>Whether any top-level argument of the call opened at <paramref name="open"/> starts with a column.</summary>
    private static bool HasColumnArgument(IReadOnlyList<SqlToken> tokens, int open, bool skipFirstArgument)
    {
        var depth = 0;
        var argumentStart = true;
        var argument = 0;
        for (var index = open; index < tokens.Count; index++)
        {
            var token = tokens[index];
            if (token.Kind == SqlTokenKind.OpenParenthesis)
            {
                depth++;
                argumentStart = depth == 1;
                continue;
            }

            if (token.Kind == SqlTokenKind.CloseParenthesis && --depth == 0)
            {
                return false;
            }

            if (depth == 1 && argumentStart && !(skipFirstArgument && argument == 0) && !TypeNames.Contains(token.Text) && IsColumnReference(tokens, index))
            {
                return true;
            }

            var isComma = depth == 1 && token.Kind == SqlTokenKind.Symbol && token.Text == ",";
            if (isComma)
            {
                argument++;
            }

            // EXTRACT(part FROM value): the value follows FROM.
            argumentStart = isComma || (depth == 1 && IsWord(token, "FROM"));
            if (argumentStart && !isComma)
            {
                argument++;
            }
        }

        return false;
    }

    private static int FindClosingParenthesis(IReadOnlyList<SqlToken> tokens, int open)
    {
        var depth = 0;
        for (var index = open; index < tokens.Count; index++)
        {
            if (tokens[index].Kind == SqlTokenKind.OpenParenthesis)
            {
                depth++;
            }
            else if (tokens[index].Kind == SqlTokenKind.CloseParenthesis && --depth == 0)
            {
                return index;
            }
        }

        return -1;
    }

    private static bool StartsWithWildcard(string pattern, bool glob)
        => pattern.Length > 0 && (glob ? pattern[0] is '*' or '?' or '[' : pattern[0] is '%' or '_');
}
