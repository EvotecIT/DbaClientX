using System;
using System.Collections.Generic;

namespace DBAClientX.QueryPlans;

/// <summary>
/// Tracks the tables each query level of SQL text reads (<c>FROM</c>, <c>JOIN</c>, <c>UPDATE</c>, <c>APPLY</c>), level by
/// level as a token scan enters and leaves parentheses, so a column in a condition can be traced to its table: through
/// its qualifier (<c>p.Name</c>, an alias or a table name, looked up from the innermost level outwards), or, unqualified,
/// to the one table its own level reads. A heuristic over tokens: unknown when a level reads several sources, or when
/// the source is a derived table, a table-valued function or a common table expression.
/// </summary>
internal sealed partial class SqlSourceScopes
{
    private static readonly HashSet<string> NotNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "WHERE", "JOIN", "LEFT", "RIGHT", "FULL", "INNER", "OUTER", "CROSS", "NATURAL", "ON", "USING", "GROUP", "ORDER",
        "LIMIT", "OFFSET", "UNION", "INTERSECT", "EXCEPT", "WINDOW", "HAVING", "SET", "VALUES", "RETURNING", "INDEXED",
        "NOT", "AS", "SELECT", "FROM", "WITH", "FETCH", "FOR", "OPTION", "DEFAULT", "OR", "LATERAL", "APPLY"
    };

    private static readonly HashSet<string> SourceTerminators = new(StringComparer.OrdinalIgnoreCase)
    {
        "WHERE", "GROUP", "ORDER", "LIMIT", "OFFSET", "UNION", "INTERSECT", "EXCEPT", "WINDOW", "HAVING",
        "SET", "VALUES", "RETURNING", "FETCH", "FOR", "OPTION"
    };

    private static readonly HashSet<string> PredicateWords = new(StringComparer.OrdinalIgnoreCase)
    {
        "AND", "OR", "NOT", "ON", "USING", "WHEN", "THEN", "ELSE", "IS", "IN", "LIKE", "ILIKE",
        "BETWEEN", "REGEXP", "RLIKE", "GLOB", "MATCH", "COLLATE"
    };

    private readonly Stack<Scope> _scopes = new();
    private readonly HashSet<string> _commonTableExpressions = new(StringComparer.OrdinalIgnoreCase);

    internal SqlSourceScopes() => _scopes.Push(new Scope(null));

    /// <summary>
    /// Finds an explicitly qualified column whose relation is outside this SQL fragment. Resolve after reading all
    /// sources because SELECT expressions precede FROM and JOIN. This is a lexical scope check, not schema binding:
    /// unqualified columns require provider metadata and are left to the server.
    /// </summary>
    internal static string? FindExternalQualifier(string sql, bool backslashStrings = false)
    {
        var tokens = SqlTokenizer.Tokenize(sql, out bool hasExecutableComments, backslashStrings: backslashStrings);
        // These comments can add or remove bindings depending on the server/version. Preserve SQL and let it bind.
        if (hasExecutableComments) return null;
        var scopes = new SqlSourceScopes();
        var queryLevels = new Stack<bool>();
        queryLevels.Push(true);
        var sourceNames = new HashSet<int>();
        var lateralSources = new HashSet<int>();
        var sourceModifiers = new HashSet<int>();
        var references = new List<(Scope Scope, string[] Qualifier)>();
        for (int index = 0; index < tokens.Count; index++)
        {
            var token = tokens[index];
            if (sourceModifiers.Contains(index)) continue;
            bool clauseToken = index == 0 || tokens[index - 1].Text != ".";
            if (token.Kind == SqlTokenKind.OpenParenthesis)
            {
                bool query = index + 1 < tokens.Count &&
                    (IsWord(tokens[index + 1], "SELECT") || IsWord(tokens[index + 1], "WITH") || IsWord(tokens[index + 1], "VALUES"));
                scopes.Open(query || lateralSources.Contains(index), inheritParent: !sourceNames.Contains(index),
                    parentSourceLimit: lateralSources.Contains(index) ? index : int.MaxValue);
                queryLevels.Push(query);
            }
            else if (token.Kind == SqlTokenKind.CloseParenthesis)
            {
                scopes.Close();
                if (queryLevels.Count > 1) queryLevels.Pop();
            }
            else if (clauseToken && queryLevels.Peek() && (IsWord(token, "UNION") || IsWord(token, "INTERSECT") || IsWord(token, "EXCEPT")))
                scopes.Restart(statement: false);
            else if (clauseToken && queryLevels.Peek() && IsWord(token, "WITH"))
                scopes.ReadCommonTableExpressions(tokens, index, sourceNames);
            else if (clauseToken && queryLevels.Peek() && (IsWord(token, "FROM") || IsJoinAt(tokens, index) || IsWord(token, "APPLY")))
                scopes.ReadSources(tokens, index, sourceNames, lateralSources, sourceModifiers);

            // Keep the full relation qualifier so equally named tables in different schemas stay distinct.
            // Qualified functions and source names are not column references. Tokens exclude strings and comments.
            if (!sourceNames.Contains(index) && IsName(token) && index + 2 < tokens.Count &&
                tokens[index + 1].Text == "." && (IsName(tokens[index + 2]) || tokens[index + 2].Text == "*") &&
                (index + 3 >= tokens.Count || tokens[index + 3].Text != "." && tokens[index + 3].Kind != SqlTokenKind.OpenParenthesis))
            {
                int first = index;
                while (first >= 2 && tokens[first - 1].Text == "." && IsName(tokens[first - 2])) first -= 2;
                // MariaDB sequence expressions name a database object, not a relation supplying a column.
                if (IsSequenceObjectReference(tokens, first, index + 3)) continue;
                var qualifier = new string[(index - first) / 2 + 1];
                for (int part = 0; part < qualifier.Length; part++) qualifier[part] = tokens[first + part * 2].Value;
                references.Add((scopes._scopes.Peek(), qualifier));
            }
        }
        foreach (var (scope, qualifier) in references)
            if (!HasQualifier(scope, qualifier)) return string.Join(".", qualifier);
        return null;
    }

    private static bool IsSequenceObjectReference(IReadOnlyList<SqlToken> tokens, int first, int end)
        => first >= 3 && IsWord(tokens[first - 1], "FOR") && IsWord(tokens[first - 2], "VALUE") &&
               (IsWord(tokens[first - 3], "NEXT") || IsWord(tokens[first - 3], "PREVIOUS")) ||
           first >= 2 && tokens[first - 1].Kind == SqlTokenKind.OpenParenthesis &&
               (IsWord(tokens[first - 2], "NEXTVAL") || IsWord(tokens[first - 2], "LASTVAL")) &&
               end < tokens.Count && tokens[end].Kind == SqlTokenKind.CloseParenthesis;

    private static bool HasQualifier(Scope? scope, IReadOnlyList<string> qualifier)
    {
        int sourceLimit = int.MaxValue;
        for (; scope != null; scope = scope.Parent)
        {
            if (scope.QualifiedSources != null)
            foreach (var (table, alias, position) in scope.QualifiedSources)
            {
                if (position >= sourceLimit) continue;
                if (qualifier.Count == 1 && string.Equals(alias ?? (table.Length > 0 ? table[table.Length - 1] : null),
                    qualifier[0], StringComparison.OrdinalIgnoreCase)) return true;
                // An unqualified source's database comes from the connection and is unavailable at compilation.
                if (alias == null && table.Length == 1 && qualifier.Count > 1 &&
                    string.Equals(table[0], qualifier[qualifier.Count - 1], StringComparison.OrdinalIgnoreCase)) return true;
                if (alias != null || table.Length != qualifier.Count) continue;
                bool equal = true;
                for (int part = 0; part < table.Length; part++)
                    equal &= string.Equals(table[part], qualifier[part], StringComparison.OrdinalIgnoreCase);
                if (equal) return true;
            }
            sourceLimit = scope.ParentSourceLimit;
        }
        return false;
    }

    /// <summary>Enters a parenthesis: a subquery starts a level of its own, other parentheses stay in the current one.</summary>
    internal void Open(bool subquery, bool inheritParent = true, int parentSourceLimit = int.MaxValue)
        => _scopes.Push(subquery ? new Scope(inheritParent ? _scopes.Peek() : _scopes.Peek().Parent,
            inheritParent ? parentSourceLimit : _scopes.Peek().ParentSourceLimit) : _scopes.Peek());

    /// <summary>Leaves a parenthesis.</summary>
    internal void Close()
    {
        if (_scopes.Count > 1)
        {
            _scopes.Pop();
        }
    }

    /// <summary>Starts a new statement, or a new <c>SELECT</c> of a compound query at the current level.</summary>
    internal void Restart(bool statement)
    {
        if (statement)
        {
            _scopes.Clear();
            _scopes.Push(new Scope(null));
            _commonTableExpressions.Clear();
            return;
        }

        var current = _scopes.Pop();
        _scopes.Push(new Scope(current.Parent, current.ParentSourceLimit));
    }

    /// <summary>
    /// Records the names a <c>WITH</c> at <paramref name="with"/> defines (<c>WITH [RECURSIVE] a [(cols)] AS (…), b AS (…)</c>),
    /// whose columns belong to no stored table. A <c>WITH (NOLOCK)</c> table hint defines none.
    /// </summary>
    internal void ReadCommonTableExpressions(IReadOnlyList<SqlToken> tokens, int with, ISet<int>? definitionOpens = null)
    {
        var position = with + 1;
        if (position < tokens.Count && IsWord(tokens[position], "RECURSIVE"))
        {
            position++;
        }

        while (position < tokens.Count && IsName(tokens[position]))
        {
            var name = tokens[position].Value;
            position++;
            if (position < tokens.Count && tokens[position].Kind == SqlTokenKind.OpenParenthesis)
            {
                position = SkipParentheses(tokens, position);
            }

            if (position >= tokens.Count || !IsWord(tokens[position], "AS"))
            {
                return;
            }

            _commonTableExpressions.Add(name);
            position++;
            while (position < tokens.Count && (IsWord(tokens[position], "NOT") || IsWord(tokens[position], "MATERIALIZED")))
            {
                position++;
            }

            // A CTE definition cannot bind aliases declared by the SELECT that consumes it.
            definitionOpens?.Add(position);
            position = SkipParentheses(tokens, position);
            if (position >= tokens.Count || tokens[position].Text != ",")
            {
                return;
            }

            position++;
        }
    }

    /// <summary>
    /// Records the sources named after the <c>FROM</c>, <c>JOIN</c>, <c>UPDATE</c> or <c>APPLY</c> at
    /// <paramref name="keyword"/> in the current level (a <c>FROM</c> list continues after commas).
    /// </summary>
    internal void ReadSources(IReadOnlyList<SqlToken> tokens, int keyword, ISet<int>? sourceNames = null, ISet<int>? lateralSources = null,
        ISet<int>? sourceModifiers = null)
    {
        var scope = _scopes.Peek();
        var isFrom = IsWord(tokens[keyword], "FROM") || tokens[keyword].Kind == SqlTokenKind.OpenParenthesis || tokens[keyword].Text == ",";
        if (sourceNames != null && IsWord(tokens[keyword], "FROM"))
            ReadSourceListTail(tokens, keyword + 1, tokens.Count, sourceNames, lateralSources, sourceModifiers);
        var position = keyword + 1;
        while (position < tokens.Count)
        {
            // ODBC's { OJ ... } escape encloses ordinary table references.
            if (position + 1 < tokens.Count && tokens[position].Text == "{" && IsWord(tokens[position + 1], "OJ")) position += 2;
            bool lateral = IsWord(tokens[position], "LATERAL");
            if (lateral) position++;
            if (position >= tokens.Count) return;
            if (!scope.SourcePositions.Add(position)) return;
            if (tokens[position].Kind == SqlTokenKind.OpenParenthesis)
            {
                int definition = position;
                bool query = position + 1 < tokens.Count && (IsWord(tokens[position + 1], "SELECT") ||
                    IsWord(tokens[position + 1], "WITH") || IsWord(tokens[position + 1], "VALUES"));
                if (query)
                {
                    if (lateral) lateralSources?.Add(position);
                    else sourceNames?.Add(position);
                }
                else ReadGroupedSources(tokens, position, sourceNames, lateralSources, sourceModifiers);
                position = SkipParentheses(tokens, position);
                position = SkipTemporalPeriod(tokens, position, sourceModifiers);
                var alias = AliasAt(tokens, ref position);
                if (query || alias != null)
                {
                    scope.Sources.Add((null, alias));
                    if (sourceNames != null)
                        (scope.QualifiedSources ??= new()).Add((Array.Empty<string>(), alias, definition));
                }
            }
            else if (IsName(tokens[position]) && (tokens[position].Kind != SqlTokenKind.Word || !NotNames.Contains(tokens[position].Value)))
            {
                int first = position;
                var table = tokens[position].Value;
                sourceNames?.Add(position);
                while (position + 2 < tokens.Count && tokens[position + 1].Text == "." && IsName(tokens[position + 2]))
                {
                    position += 2;
                    sourceNames?.Add(position);
                    table = tokens[position].Value;
                }

                string[]? qualifiedTable = null;
                int definition = first;
                if (sourceNames != null)
                {
                    qualifiedTable = new string[(position - first) / 2 + 1];
                    for (int part = 0; part < qualifiedTable.Length; part++) qualifiedTable[part] = tokens[first + part * 2].Value;
                }
                position++;
                var derived = _commonTableExpressions.Contains(table);
                if (position < tokens.Count && tokens[position].Kind == SqlTokenKind.OpenParenthesis)
                {
                    // A table-valued function such as pragma_table_info(...) or json_each(...).
                    derived = true;
                    if (IsWord(tokens[first], "JSON_TABLE"))
                    {
                        definition = position;
                        lateralSources?.Add(position);
                    }
                    position = SkipParentheses(tokens, position);
                }

                if (position + 1 < tokens.Count && IsWord(tokens[position], "PARTITION") &&
                    tokens[position + 1].Kind == SqlTokenKind.OpenParenthesis)
                {
                    int modifier = position;
                    position = SkipParentheses(tokens, position + 1);
                    MarkRange(sourceModifiers, modifier, position);
                }
                position = SkipTemporalPeriod(tokens, position, sourceModifiers);
                var alias = AliasAt(tokens, ref position);
                position = SkipIndexHints(tokens, position, sourceModifiers);
                if (position + 1 < tokens.Count && IsWord(tokens[position], "WITH") && tokens[position + 1].Kind == SqlTokenKind.OpenParenthesis)
                {
                    // A table hint: WITH (NOLOCK).
                    position = SkipParentheses(tokens, position + 1);
                }

                scope.Sources.Add((derived ? null : table, alias ?? (derived ? table : null)));
                if (qualifiedTable != null)
                    (scope.QualifiedSources ??= new()).Add((qualifiedTable, alias, definition));
            }

            if (!isFrom || position >= tokens.Count || tokens[position].Text != ",")
            {
                return;
            }

            position++;
        }
    }

    private void ReadGroupedSources(IReadOnlyList<SqlToken> tokens, int open, ISet<int>? sourceNames, ISet<int>? lateralSources,
        ISet<int>? sourceModifiers)
    {
        // Parenthesized table factors and joins share their enclosing SELECT's relation scope.
        ReadSources(tokens, open, sourceNames, lateralSources, sourceModifiers);
        int end = SkipParentheses(tokens, open) - 1;
        ReadSourceListTail(tokens, open + 1, end, sourceNames, lateralSources, sourceModifiers);
    }

    private void ReadSourceListTail(IReadOnlyList<SqlToken> tokens, int first, int end, ISet<int>? sourceNames, ISet<int>? lateralSources,
        ISet<int>? sourceModifiers)
    {
        for (int index = first; index < end; index++)
        {
            int afterHints = SkipIndexHints(tokens, index, sourceModifiers);
            if (afterHints > index) { index = afterHints - 1; continue; }
            int afterPeriod = SkipTemporalPeriod(tokens, index, sourceModifiers);
            if (afterPeriod > index) { index = afterPeriod - 1; continue; }
            if (tokens[index].Kind == SqlTokenKind.OpenParenthesis) index = SkipParentheses(tokens, index) - 1;
            else if (tokens[index].Kind == SqlTokenKind.CloseParenthesis || tokens[index].Kind == SqlTokenKind.Semicolon ||
                tokens[index].Kind == SqlTokenKind.Word && SourceTerminators.Contains(tokens[index].Text)) return;
            else if (IsJoinAt(tokens, index) || tokens[index].Text == ",")
                ReadSources(tokens, index, sourceNames, lateralSources, sourceModifiers);
        }
    }

    private static int SkipIndexHints(IReadOnlyList<SqlToken> tokens, int position, ISet<int>? sourceModifiers)
    {
        while (position + 1 < tokens.Count &&
            (IsWord(tokens[position], "USE") || IsWord(tokens[position], "FORCE") || IsWord(tokens[position], "IGNORE")) &&
            (IsWord(tokens[position + 1], "INDEX") || IsWord(tokens[position + 1], "KEY")))
        {
            int start = position;
            int next = position + 2;
            if (next + 1 < tokens.Count && IsWord(tokens[next], "FOR"))
            {
                next++;
                if (IsWord(tokens[next], "JOIN")) next++;
                else if (next + 1 < tokens.Count && (IsWord(tokens[next], "ORDER") || IsWord(tokens[next], "GROUP")) &&
                    IsWord(tokens[next + 1], "BY")) next += 2;
                else return position;
            }
            if (next >= tokens.Count || tokens[next].Kind != SqlTokenKind.OpenParenthesis) return position;
            position = SkipParentheses(tokens, next);
            MarkRange(sourceModifiers, start, position);
        }
        return position;
    }

    private static void MarkRange(ISet<int>? positions, int first, int end)
    {
        if (positions == null) return;
        for (int position = first; position < end; position++) positions.Add(position);
    }

    /// <summary>
    /// The table a column belongs to: the source its qualifier names (an alias first, then a table name, innermost level
    /// first), or for an unqualified column the one source of its own level; null when it cannot be told or the source
    /// is not a stored table.
    /// </summary>
    internal string? TableOf(string? qualifier)
    {
        if (qualifier == null)
        {
            var own = _scopes.Peek();
            return own.Sources.Count == 1 ? own.Sources[0].Table : null;
        }

        for (var scope = _scopes.Peek(); scope != null; scope = scope.Parent)
        {
            foreach (var (table, alias) in scope.Sources)
            {
                if (string.Equals(alias, qualifier, StringComparison.OrdinalIgnoreCase))
                {
                    return table;
                }
            }

            foreach (var (table, alias) in scope.Sources)
            {
                if (alias == null && string.Equals(table, qualifier, StringComparison.OrdinalIgnoreCase))
                {
                    return table;
                }
            }
        }

        return null;
    }

    private static string? AliasAt(IReadOnlyList<SqlToken> tokens, ref int position)
    {
        var start = position;
        if (position < tokens.Count && IsWord(tokens[position], "AS"))
        {
            position++;
        }

        if (position < tokens.Count && IsName(tokens[position]) && !IsIndexHintAt(tokens, position) && !IsJoinAt(tokens, position) &&
            (tokens[position].Kind != SqlTokenKind.Word || !NotNames.Contains(tokens[position].Value)))
        {
            return tokens[position++].Value;
        }

        position = start;
        return null;
    }

    private static int SkipParentheses(IReadOnlyList<SqlToken> tokens, int position)
    {
        var depth = 0;
        for (; position < tokens.Count; position++)
        {
            if (tokens[position].Kind == SqlTokenKind.OpenParenthesis)
            {
                depth++;
            }
            else if (tokens[position].Kind == SqlTokenKind.CloseParenthesis && --depth == 0)
            {
                return position + 1;
            }
        }

        return position;
    }

    private static bool IsName(SqlToken token) => token.Kind is SqlTokenKind.Word or SqlTokenKind.QuotedIdentifier;

    private static bool IsWord(SqlToken token, string word)
        => token.Kind == SqlTokenKind.Word && string.Equals(token.Text, word, StringComparison.OrdinalIgnoreCase);

    internal static bool IsJoinAt(IReadOnlyList<SqlToken> tokens, int index)
        => (index == 0 || tokens[index - 1].Text is not ("." or "," or "(") &&
            !IsWord(tokens[index - 1], "FROM") && !IsWord(tokens[index - 1], "AS") &&
            !IsWord(tokens[index - 1], "SELECT") && !IsWord(tokens[index - 1], "JOIN") &&
            !IsWord(tokens[index - 1], "STRAIGHT_JOIN") && !IsWord(tokens[index - 1], "APPLY") &&
            !IsWord(tokens[index - 1], "LATERAL") && !IsWord(tokens[index - 1], "OJ")) &&
            (IsWord(tokens[index], "JOIN") || IsWord(tokens[index], "STRAIGHT_JOIN") && index + 1 < tokens.Count &&
            (tokens[index + 1].Kind == SqlTokenKind.OpenParenthesis || tokens[index + 1].Text == "{" ||
                IsName(tokens[index + 1]) && !NotNames.Contains(tokens[index + 1].Value) &&
                !PredicateWords.Contains(tokens[index + 1].Value)) && IsSourceClauseBefore(tokens, index));

    // STRAIGHT_JOIN is also a portable column/function name. Look for its source-clause context
    // only when that word occurs; ordinary tokens add no scan or allocation.
    private static bool IsSourceClauseBefore(IReadOnlyList<SqlToken> tokens, int index)
    {
        if (index > 0 && tokens[index - 1].Kind == SqlTokenKind.Word && PredicateWords.Contains(tokens[index - 1].Text)) return false;
        int depth = 0;
        for (int previous = index - 1; previous >= 0; previous--)
        {
            var token = tokens[previous];
            if (token.Kind == SqlTokenKind.CloseParenthesis) { depth++; continue; }
            if (token.Kind == SqlTokenKind.OpenParenthesis) { if (depth > 0) depth--; continue; }
            if (depth != 0 || token.Kind != SqlTokenKind.Word || previous > 0 && tokens[previous - 1].Text == "." ||
                previous + 1 < tokens.Count && tokens[previous + 1].Text == ".") continue;
            if (SourceTerminators.Contains(token.Text) || IsWord(token, "SELECT")) return false;
            if (IsWord(token, "FROM") || IsWord(token, "UPDATE") || IsWord(token, "JOIN")) return true;
        }
        return false;
    }

    private sealed class Scope
    {
        internal Scope(Scope? parent, int parentSourceLimit = int.MaxValue)
        {
            Parent = parent;
            ParentSourceLimit = parentSourceLimit;
        }

        internal Scope? Parent { get; }

        internal int ParentSourceLimit { get; }

        /// <summary>The sources of the level: a stored table (or null for a derived one) and its alias.</summary>
        internal List<(string? Table, string? Alias)> Sources { get; } = new();

        internal HashSet<int> SourcePositions { get; } = new();

        /// <summary>Full relation identities, allocated only for the explicit-qualifier validation scan.</summary>
        internal List<(string[] Table, string? Alias, int Position)>? QualifiedSources { get; set; }
    }
}
