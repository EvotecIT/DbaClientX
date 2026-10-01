using System;
using System.Collections.Generic;

namespace DBAClientX.QueryPlans;

/// <summary>
/// Finds predicates in SQL text that an index probably cannot serve: a function wrapped around a column in a
/// <c>WHERE</c> or <c>ON</c> condition, a <c>COLLATE</c> applied to a comparison with a column, and <c>LIKE</c>/<c>GLOB</c>
/// patterns that start with a wildcard.
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
        "INSTR", "PRINTF", "FORMAT", "CONCAT", "UNICODE", "TYPEOF",
        // DbaClientX's Unicode folding functions for SQLite (SQLiteUnicodeText).
        "DBX_LOWER", "DBX_UPPER"
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
    public static IReadOnlyList<SqlSargabilityFinding> Analyze(string sql) => Analyze(sql, options: null);

    /// <summary>
    /// Returns the predicates of <paramref name="sql"/> that an index probably cannot serve, knowing the functions a
    /// connection registers and the collations its indexes use.
    /// </summary>
    /// <param name="sql">One or more SQL statements.</param>
    /// <param name="options">Registered functions to treat like built-in ones, and the collation of each column's index;
    /// <see langword="null"/> for none.</param>
    /// <returns>The findings in text order; empty when none are found.</returns>
    public static IReadOnlyList<SqlSargabilityFinding> Analyze(string sql, SqlSargabilityOptions? options)
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

            if (IsWord(token, "COLLATE") && index + 1 < tokens.Count)
            {
                var finding = CollationFinding(sql, tokens, index, options);
                if (finding != null)
                {
                    findings.Add(finding);
                }
            }
            else if (token.Kind == SqlTokenKind.Word && IsWrappingFunction(token.Text, options) &&
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

    private static bool IsWrappingFunction(string name, SqlSargabilityOptions? options)
        => WrappingFunctions.Contains(name) || (options != null && options.Functions.Contains(name));

    /// <summary>
    /// A <c>COLLATE</c> at <paramref name="collate"/> that applies to a comparison with a column: after the column
    /// (<c>Name COLLATE NOCASE = @p</c>) or after the value compared with it (<c>Name = @p COLLATE NOCASE</c> or
    /// <c>@p COLLATE NOCASE = Name</c>, which SQLite applies to the comparison too). Unless the column's index uses that
    /// collation, the index cannot serve it. <c>IS [NOT] NULL</c> ignores collations, and a collation equal to the
    /// column's (from <see cref="SqlSargabilityOptions"/>) is not reported.
    /// </summary>
    private static SqlSargabilityFinding? CollationFinding(string sql, IReadOnlyList<SqlToken> tokens, int collate, SqlSargabilityOptions? options)
    {
        if (collate == 0 || !IsName(tokens[collate + 1]))
        {
            return null;
        }

        // A schema-qualified collation (pg_catalog."default") is matched by its last part.
        var last = collate + 1;
        while (last + 2 < tokens.Count && tokens[last + 1].Text == "." && IsName(tokens[last + 2]))
        {
            last += 2;
        }

        var collation = tokens[last].Value;
        var after = last + 1;
        if (after < tokens.Count && (IsWord(tokens[after], "ISNULL") || IsWord(tokens[after], "NOTNULL") ||
            (IsWord(tokens[after], "NOT") && after + 1 < tokens.Count && IsWord(tokens[after + 1], "NULL")) ||
            (IsWord(tokens[after], "IS") && after + 1 < tokens.Count && (IsWord(tokens[after + 1], "NULL") ||
             (IsWord(tokens[after + 1], "NOT") && after + 2 < tokens.Count && IsWord(tokens[after + 2], "NULL"))))))
        {
            return null;
        }

        int first;
        int end;
        int column;
        if (IsValue(tokens[collate - 1]))
        {
            var start = collate - 2;
            while (start >= 0 && IsComparison(tokens[start]))
            {
                start--;
            }

            if (start >= 0 && start < collate - 2)
            {
                // Column <op> value COLLATE name.
                column = start;
                first = QualifiedStart(tokens, start);
                end = last;
            }
            else
            {
                // Value COLLATE name <op> column.
                var next = after;
                while (next < tokens.Count && IsComparison(tokens[next]))
                {
                    next++;
                }

                if (next == after || next >= tokens.Count)
                {
                    return null;
                }

                column = next;
                while (column + 2 < tokens.Count && tokens[column + 1].Text == "." && IsColumnReference(tokens, column + 2))
                {
                    column += 2;
                }

                first = collate - 1;
                end = column;
            }
        }
        else
        {
            column = collate - 1;
            first = QualifiedStart(tokens, column);
            end = last;
        }

        var reference = tokens[column];
        if (!IsColumnReference(tokens, column) || IsWord(reference, "END") || ClauseKeywords.Contains(reference.Text))
        {
            return null;
        }

        var indexed = options != null && options.ColumnCollations.TryGetValue(reference.Value, out var declared)
            ? declared
            : options == null ? SqlSargabilityOptions.SQLiteDefaultCollation : options.DefaultCollation;
        // PostgreSQL's "default" is the column's own collation.
        if (string.Equals(indexed, collation, StringComparison.OrdinalIgnoreCase) || string.Equals(collation, "default", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        return new SqlSargabilityFinding(
            SqlSargabilityFindingKind.CollationOnColumn,
            tokens[first].Position,
            sql.Substring(tokens[first].Position, tokens[end].Position + tokens[end].Text.Length - tokens[first].Position),
            $"COLLATE {collation} compares {reference.Value} under a collation its index may not use, and an index serves a comparison only under its own collation; index the column with that collation (or declare it in SqlSargabilityOptions.ColumnCollations), or compare a stored folded column.");
    }

    private static bool IsName(SqlToken token) => token.Kind is SqlTokenKind.Word or SqlTokenKind.QuotedIdentifier or SqlTokenKind.String;

    private static bool IsValue(SqlToken token) => token.Kind is SqlTokenKind.Parameter or SqlTokenKind.String or SqlTokenKind.Number;

    private static bool IsComparison(SqlToken token)
        => (token.Kind == SqlTokenKind.Symbol && token.Text is "=" or "<" or ">" or "!") ||
           IsWord(token, "IS") || IsWord(token, "NOT") || IsWord(token, "LIKE") || IsWord(token, "GLOB");

    /// <summary>The first token of a column reference ending at <paramref name="column"/> (<c>t.Name</c> starts at <c>t</c>).</summary>
    private static int QualifiedStart(IReadOnlyList<SqlToken> tokens, int column)
    {
        while (column > 1 && tokens[column - 1].Text == "." && IsColumnReference(tokens, column - 2))
        {
            column -= 2;
        }

        return column;
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
