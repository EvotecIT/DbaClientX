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
/// <para>A function counts when a column appears anywhere in its arguments, nested calls included
/// (<c>dbx_lower(NULLIF(Name, ''))</c>) but not subqueries, and the outermost known function around it is the finding.
/// A <c>COLLATE</c> after a call around a column (<c>CAST(Name AS TEXT) COLLATE NOCASE</c>) is a collation finding
/// besides the function. Findings name the column and, when the text shows it, its table
/// (<see cref="SqlSargabilityFinding.Column"/>, <see cref="SqlSargabilityFinding.Table"/>).</para>
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
        // Whether each level is inside the arguments of a function already reported, whose inner calls are part of it.
        var reported = new Stack<bool>();
        reported.Push(false);
        var reportedCallOpens = false;
        // Whether each level is a query (the statement or a subquery) rather than a call's or a group's parentheses.
        var queryLevel = new Stack<bool>();
        queryLevel.Push(true);
        var scopes = new SqlSourceScopes();
        for (var index = 0; index < tokens.Count; index++)
        {
            var token = tokens[index];
            switch (token.Kind)
            {
                case SqlTokenKind.OpenParenthesis:
                    var subquery = index + 1 < tokens.Count && (IsWord(tokens[index + 1], "SELECT") || IsWord(tokens[index + 1], "WITH") || IsWord(tokens[index + 1], "VALUES"));
                    inCondition.Push(inCondition.Peek());
                    joinPending.Push(false);
                    reported.Push(reportedCallOpens || (reported.Peek() && !subquery));
                    reportedCallOpens = false;
                    queryLevel.Push(subquery);
                    scopes.Open(subquery);
                    continue;
                case SqlTokenKind.CloseParenthesis:
                    if (inCondition.Count > 1)
                    {
                        inCondition.Pop();
                        joinPending.Pop();
                        reported.Pop();
                        queryLevel.Pop();
                        scopes.Close();
                    }

                    continue;
                case SqlTokenKind.Semicolon:
                    inCondition.Clear();
                    inCondition.Push(false);
                    joinPending.Clear();
                    joinPending.Push(false);
                    reported.Clear();
                    reported.Push(false);
                    queryLevel.Clear();
                    queryLevel.Push(true);
                    scopes.Restart(statement: true);
                    continue;
                case SqlTokenKind.Word when IsWord(token, "WHERE") || (IsWord(token, "ON") && joinPending.Peek()):
                    SetTop(joinPending, false);
                    SetCondition(inCondition, true);
                    continue;
                case SqlTokenKind.Word when IsWord(token, "ON"):
                    continue;
                case SqlTokenKind.Word when IsWord(token, "WITH") && queryLevel.Peek():
                    scopes.ReadCommonTableExpressions(tokens, index);
                    continue;
                case SqlTokenKind.Word when IsWord(token, "APPLY"):
                    // CROSS/OUTER APPLY adds the columns of a derived source.
                    scopes.ReadSources(tokens, index);
                    continue;
                case SqlTokenKind.Word when IsWord(token, "FROM") && index > 0 && IsWord(tokens[index - 1], "DISTINCT"):
                    // IS [NOT] DISTINCT FROM compares; it starts no clause.
                    break;
                case SqlTokenKind.Word when ClauseKeywords.Contains(token.Text) || IsWord(token, "UPDATE"):
                    SetTop(joinPending, IsWord(token, "JOIN"));
                    SetCondition(inCondition, false);
                    if (!queryLevel.Peek())
                    {
                        // FROM inside EXTRACT(YEAR FROM x), SUBSTRING or TRIM names no table.
                    }
                    else if (IsWord(token, "FROM") || IsWord(token, "JOIN") || IsWord(token, "UPDATE"))
                    {
                        scopes.ReadSources(tokens, index);
                    }
                    else if (IsWord(token, "UNION") || IsWord(token, "INTERSECT") || IsWord(token, "EXCEPT"))
                    {
                        scopes.Restart(statement: false);
                    }

                    continue;
            }

            if (!inCondition.Peek())
            {
                continue;
            }

            if (IsWord(token, "COLLATE") && index + 1 < tokens.Count)
            {
                var finding = CollationFinding(sql, tokens, index, options, scopes);
                if (finding != null)
                {
                    findings.Add(finding);
                }
            }
            else if (!reported.Peek() && token.Kind == SqlTokenKind.Word && IsWrappingFunction(token.Text, options) &&
                     index + 2 < tokens.Count && tokens[index + 1].Kind == SqlTokenKind.OpenParenthesis &&
                     FindColumnInCall(tokens, index + 1, SkipsFirstArgument(token.Text)) is var column and >= 0)
            {
                // The outermost function around the column is the expression an index would need; calls inside it
                // (dbx_lower(NULLIF(Name, ''))) are part of that one finding.
                var end = FindClosingParenthesis(tokens, index + 1);
                var text = end < 0 ? sql.Substring(token.Position) : sql.Substring(token.Position, tokens[end].Position + 1 - token.Position);
                var (name, table) = ColumnAndTable(tokens, column, scopes);
                findings.Add(new SqlSargabilityFinding(
                    SqlSargabilityFindingKind.FunctionOnColumn,
                    token.Position,
                    text,
                    $"{token.Text.ToUpperInvariant()}() wraps a column{Describe(name, table)} in a condition, so an index on that column cannot serve it; compare the stored value, store a normalized column, or index the expression.",
                    name,
                    table));
                reportedCallOpens = true;
            }
            else if (!reported.Peek() && IsColumnReference(tokens, index) && index + 1 < tokens.Count && tokens[index + 1].Text == "::")
            {
                var end = Math.Min(index + 2, tokens.Count - 1);
                var first = QualifiedStart(tokens, index);
                var (name, table) = ColumnAndTable(tokens, index, scopes);
                findings.Add(new SqlSargabilityFinding(
                    SqlSargabilityFindingKind.FunctionOnColumn,
                    tokens[first].Position,
                    sql.Substring(tokens[first].Position, tokens[end].Position + tokens[end].Text.Length - tokens[first].Position),
                    $"A :: cast converts a column{Describe(name, table)} in a condition, so an index on that column cannot serve it; cast the compared value instead.",
                    name,
                    table));
            }
            else if (token.Kind == SqlTokenKind.Word && (IsWord(token, "LIKE") || IsWord(token, "ILIKE") || IsWord(token, "GLOB")) &&
                     index + 1 < tokens.Count && tokens[index + 1].Kind == SqlTokenKind.String &&
                     StartsWithWildcard(tokens[index + 1].Value, IsWord(token, "GLOB")))
            {
                var pattern = tokens[index + 1];
                var subject = index > 0 && IsWord(tokens[index - 1], "NOT") ? index - 2 : index - 1;
                var (name, table) = subject >= 0 && IsColumnReference(tokens, subject) ? ColumnAndTable(tokens, subject, scopes) : (null, null);
                findings.Add(new SqlSargabilityFinding(
                    SqlSargabilityFindingKind.LeadingWildcard,
                    token.Position,
                    sql.Substring(token.Position, pattern.Position + pattern.Text.Length - token.Position),
                    $"{token.Text.ToUpperInvariant()} pattern starts with a wildcard{Describe(name, table)}, so an index cannot narrow it and every row is read; use a prefix pattern or a full-text index.",
                    name,
                    table));
            }
        }

        return findings;
    }

    /// <summary>The unqualified column at <paramref name="column"/> and the table its qualifier or query level names.</summary>
    private static (string? Column, string? Table) ColumnAndTable(IReadOnlyList<SqlToken> tokens, int column, SqlSourceScopes scopes)
    {
        string? qualifier = column >= 2 && tokens[column - 1].Text == "." && IsName(tokens[column - 2]) ? tokens[column - 2].Value : null;
        return (tokens[column].Value, scopes.TableOf(qualifier));
    }

    /// <summary>The column in parentheses after a message's subject (<c> (ProbeResults.ProbeName)</c>), or nothing when unknown.</summary>
    private static string Describe(string? column, string? table)
        => column == null ? string.Empty : table == null ? $" ({column})" : $" ({table}.{column})";

    private static bool IsWrappingFunction(string name, SqlSargabilityOptions? options)
        => WrappingFunctions.Contains(name) || (options != null && options.Functions.Contains(name));

    /// <summary>
    /// A <c>COLLATE</c> at <paramref name="collate"/> that applies to a comparison with a column: after the column
    /// (<c>Name COLLATE NOCASE = @p</c>) or after the value compared with it (<c>Name = @p COLLATE NOCASE</c> or
    /// <c>@p COLLATE NOCASE = Name</c>, which SQLite applies to the comparison too). Unless the column's index uses that
    /// collation, the index cannot serve it. <c>IS [NOT] NULL</c> ignores collations, and a collation equal to the
    /// column's (from <see cref="SqlSargabilityOptions"/>) is not reported.
    /// </summary>
    private static SqlSargabilityFinding? CollationFinding(string sql, IReadOnlyList<SqlToken> tokens, int collate, SqlSargabilityOptions? options, SqlSourceScopes scopes)
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

        // The column side is a column or a call or group around one (CAST(Name AS TEXT) COLLATE NOCASE = @p).
        (int Column, int First, int End)? operand;
        int first;
        int end;
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
                operand = OperandEndingAt(tokens, start);
                first = operand?.First ?? 0;
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

                operand = OperandStartingAt(tokens, next);
                first = collate - 1;
                end = operand?.End ?? 0;
            }
        }
        else
        {
            operand = OperandEndingAt(tokens, collate - 1);
            first = operand?.First ?? 0;
            end = last;
        }

        if (operand == null)
        {
            return null;
        }

        var column = operand.Value.Column;
        var reference = tokens[column];
        if (IsWord(reference, "END") || ClauseKeywords.Contains(reference.Text))
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

        var (name, table) = ColumnAndTable(tokens, column, scopes);
        return new SqlSargabilityFinding(
            SqlSargabilityFindingKind.CollationOnColumn,
            tokens[first].Position,
            sql.Substring(tokens[first].Position, tokens[end].Position + tokens[end].Text.Length - tokens[first].Position),
            $"COLLATE {collation} compares {name}{(table == null ? string.Empty : " of " + table)} under a collation its index may not use, and an index serves a comparison only under its own collation; index the column with that collation (or declare it in SqlSargabilityOptions.ColumnCollations), or compare a stored folded column.",
            name,
            table);
    }

    /// <summary>
    /// The column operand ending at <paramref name="index"/>: a column (<c>t.Name</c>), or a call or parenthesized
    /// expression around one (<c>CAST(Name AS TEXT)</c>), with the column found inside it.
    /// </summary>
    private static (int Column, int First, int End)? OperandEndingAt(IReadOnlyList<SqlToken> tokens, int index)
    {
        if (index < 0)
        {
            return null;
        }

        if (tokens[index].Kind == SqlTokenKind.CloseParenthesis)
        {
            var open = FindOpeningParenthesis(tokens, index);
            var call = open > 0 && IsFunctionName(tokens[open - 1]);
            if (open < 0 || IsSubqueryOrCaseGroup(tokens, open, call))
            {
                return null;
            }

            var column = FindColumnInCall(tokens, open, call && SkipsFirstArgument(tokens[open - 1].Text));
            return column < 0 ? null : (column, call ? open - 1 : open, index);
        }

        return IsColumnReference(tokens, index) ? (index, QualifiedStart(tokens, index), index) : null;
    }

    /// <summary>Whether a word before <c>(</c> is a function name rather than a keyword (<c>AND (</c>, <c>IN (</c>, <c>WHERE (</c>).</summary>
    private static bool IsFunctionName(SqlToken token)
        => token.Kind == SqlTokenKind.Word && !OperandKeywords.Contains(token.Text) && !ClauseKeywords.Contains(token.Text) &&
           !IsWord(token, "WHERE") && !IsWord(token, "ON") && !IsWord(token, "ANY") && !IsWord(token, "SOME");

    /// <summary>
    /// Whether the parentheses at <paramref name="open"/> hold a subquery, whose columns are not this expression's, or
    /// are a plain group around a <c>CASE</c> expression, whose collation the analyzer leaves alone as it does without
    /// parentheses (a call around one, <c>COALESCE(CASE … END, '')</c>, is a call around its columns).
    /// </summary>
    private static bool IsSubqueryOrCaseGroup(IReadOnlyList<SqlToken> tokens, int open, bool call)
        => open + 1 < tokens.Count && (IsWord(tokens[open + 1], "SELECT") || IsWord(tokens[open + 1], "WITH") ||
                                       IsWord(tokens[open + 1], "VALUES") || (!call && IsWord(tokens[open + 1], "CASE")));

    /// <summary>The column operand starting at <paramref name="index"/> (see <see cref="OperandEndingAt"/>).</summary>
    private static (int Column, int First, int End)? OperandStartingAt(IReadOnlyList<SqlToken> tokens, int index)
    {
        var open = tokens[index].Kind == SqlTokenKind.OpenParenthesis
            ? index
            : IsFunctionName(tokens[index]) && index + 1 < tokens.Count && tokens[index + 1].Kind == SqlTokenKind.OpenParenthesis ? index + 1 : -1;
        if (open >= 0)
        {
            if (IsSubqueryOrCaseGroup(tokens, open, call: open > index))
            {
                return null;
            }

            var close = FindClosingParenthesis(tokens, open);
            var column = FindColumnInCall(tokens, open, open > index && SkipsFirstArgument(tokens[index].Text));
            return column < 0 || close < 0 ? null : (column, index, close);
        }

        var last = index;
        while (last + 2 < tokens.Count && tokens[last + 1].Text == "." && IsColumnReference(tokens, last + 2))
        {
            last += 2;
        }

        return IsColumnReference(tokens, last) ? (last, index, last) : null;
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

    private static int FindOpeningParenthesis(IReadOnlyList<SqlToken> tokens, int close)
    {
        var depth = 0;
        for (var index = close; index >= 0; index--)
        {
            if (tokens[index].Kind == SqlTokenKind.CloseParenthesis)
            {
                depth++;
            }
            else if (tokens[index].Kind == SqlTokenKind.OpenParenthesis && --depth == 0)
            {
                return index;
            }
        }

        return -1;
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
