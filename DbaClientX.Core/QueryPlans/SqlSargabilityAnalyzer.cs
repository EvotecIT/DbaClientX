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
public static partial class SqlSargabilityAnalyzer
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
        var sourceModifiers = new HashSet<int>();
        for (var index = 0; index < tokens.Count; index++)
        {
            var token = tokens[index];
            if (sourceModifiers.Contains(index)) continue;
            bool clauseToken = index == 0 || tokens[index - 1].Text != ".";
            bool joinToken = token.Kind == SqlTokenKind.Word && clauseToken && SqlSourceScopes.IsJoinAt(tokens, index);
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
                case SqlTokenKind.Word when clauseToken && (IsWord(token, "WHERE") || (IsWord(token, "ON") && joinPending.Peek())):
                    SetTop(joinPending, false);
                    SetCondition(inCondition, true);
                    continue;
                case SqlTokenKind.Word when clauseToken && IsWord(token, "ON"):
                    continue;
                case SqlTokenKind.Word when clauseToken && IsWord(token, "WITH") && queryLevel.Peek():
                    scopes.ReadCommonTableExpressions(tokens, index);
                    continue;
                case SqlTokenKind.Word when clauseToken && IsWord(token, "APPLY"):
                    // CROSS/OUTER APPLY adds the columns of a derived source.
                    scopes.ReadSources(tokens, index, sourceModifiers: sourceModifiers);
                    continue;
                case SqlTokenKind.Word when clauseToken && IsWord(token, "FROM") && index > 0 && IsWord(tokens[index - 1], "DISTINCT"):
                    // IS [NOT] DISTINCT FROM compares; it starts no clause.
                    break;
                case SqlTokenKind.Word when clauseToken && (ClauseKeywords.Contains(token.Text) || joinToken || IsWord(token, "UPDATE")):
                    SetTop(joinPending, joinToken);
                    SetCondition(inCondition, false);
                    if (!queryLevel.Peek())
                    {
                        // FROM inside EXTRACT(YEAR FROM x), SUBSTRING or TRIM names no table.
                    }
                    else if (IsWord(token, "FROM") || joinToken || IsWord(token, "UPDATE"))
                    {
                        scopes.ReadSources(tokens, index, sourceModifiers: sourceModifiers);
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
                findings.Add(FunctionFinding(sql, tokens, index, column, scopes));
                reportedCallOpens = true;
            }
            else if (!reported.Peek() && IsColumnReference(tokens, index) && index + 1 < tokens.Count && tokens[index + 1].Text == "::")
            {
                findings.Add(CastFinding(sql, tokens, index, scopes));
            }
            else if (token.Kind == SqlTokenKind.Word && (IsWord(token, "LIKE") || IsWord(token, "ILIKE") || IsWord(token, "GLOB")) &&
                     index + 1 < tokens.Count && tokens[index + 1].Kind == SqlTokenKind.String &&
                     StartsWithWildcard(tokens[index + 1].Value, IsWord(token, "GLOB")))
            {
                findings.Add(LeadingWildcardFinding(sql, tokens, index, scopes));
            }
        }

        return findings;
    }

    private static bool IsWrappingFunction(string name, SqlSargabilityOptions? options)
        => WrappingFunctions.Contains(name) || (options != null && options.Functions.Contains(name));

    private static void SetCondition(Stack<bool> inCondition, bool value) => SetTop(inCondition, value);

    private static void SetTop(Stack<bool> stack, bool value)
    {
        stack.Pop();
        stack.Push(value);
    }
}
