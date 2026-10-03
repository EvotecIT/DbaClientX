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
internal sealed class SqlSourceScopes
{
    private static readonly HashSet<string> NotNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "WHERE", "JOIN", "LEFT", "RIGHT", "FULL", "INNER", "OUTER", "CROSS", "NATURAL", "ON", "USING", "GROUP", "ORDER",
        "LIMIT", "OFFSET", "UNION", "INTERSECT", "EXCEPT", "WINDOW", "HAVING", "SET", "VALUES", "RETURNING", "INDEXED",
        "NOT", "AS", "SELECT", "FROM", "WITH", "FETCH", "FOR", "OPTION", "DEFAULT", "OR", "LATERAL", "APPLY"
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
        var tokens = SqlTokenizer.Tokenize(sql, backslashStrings: backslashStrings);
        var scopes = new SqlSourceScopes();
        var queryLevels = new Stack<bool>();
        queryLevels.Push(true);
        var sourceNames = new HashSet<int>();
        var lateralSources = new HashSet<int>();
        var references = new List<(Scope Scope, string[] Qualifier)>();
        for (int index = 0; index < tokens.Count; index++)
        {
            var token = tokens[index];
            if (token.Kind == SqlTokenKind.OpenParenthesis)
            {
                bool query = index + 1 < tokens.Count &&
                    (IsWord(tokens[index + 1], "SELECT") || IsWord(tokens[index + 1], "WITH") || IsWord(tokens[index + 1], "VALUES"));
                scopes.Open(query, inheritParent: !sourceNames.Contains(index),
                    parentSourceLimit: lateralSources.Contains(index) ? index : int.MaxValue);
                queryLevels.Push(query);
            }
            else if (token.Kind == SqlTokenKind.CloseParenthesis)
            {
                scopes.Close();
                if (queryLevels.Count > 1) queryLevels.Pop();
            }
            else if (queryLevels.Peek() && (IsWord(token, "UNION") || IsWord(token, "INTERSECT") || IsWord(token, "EXCEPT")))
                scopes.Restart(statement: false);
            else if (queryLevels.Peek() && IsWord(token, "WITH"))
                scopes.ReadCommonTableExpressions(tokens, index, sourceNames);
            else if (queryLevels.Peek() && (IsWord(token, "FROM") || IsWord(token, "JOIN") || IsWord(token, "APPLY")))
                scopes.ReadSources(tokens, index, sourceNames, lateralSources);

            // Keep the full relation qualifier so equally named tables in different schemas stay distinct.
            // Qualified functions and source names are not column references. Tokens exclude strings and comments.
            if (!sourceNames.Contains(index) && IsName(token) && index + 2 < tokens.Count &&
                tokens[index + 1].Text == "." && (IsName(tokens[index + 2]) || tokens[index + 2].Text == "*") &&
                (index + 3 >= tokens.Count || tokens[index + 3].Text != "." && tokens[index + 3].Kind != SqlTokenKind.OpenParenthesis))
            {
                int first = index;
                while (first >= 2 && tokens[first - 1].Text == "." && IsName(tokens[first - 2])) first -= 2;
                var qualifier = new string[(index - first) / 2 + 1];
                for (int part = 0; part < qualifier.Length; part++) qualifier[part] = tokens[first + part * 2].Value;
                references.Add((scopes._scopes.Peek(), qualifier));
            }
        }
        foreach (var (scope, qualifier) in references)
            if (!HasQualifier(scope, qualifier)) return string.Join(".", qualifier);
        return null;
    }

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
        => _scopes.Push(subquery ? new Scope(inheritParent ? _scopes.Peek() : null, parentSourceLimit) : _scopes.Peek());

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
    internal void ReadSources(IReadOnlyList<SqlToken> tokens, int keyword, ISet<int>? sourceNames = null, ISet<int>? lateralSources = null)
    {
        var scope = _scopes.Peek();
        var isFrom = IsWord(tokens[keyword], "FROM") || tokens[keyword].Kind == SqlTokenKind.OpenParenthesis;
        var position = keyword + 1;
        while (position < tokens.Count)
        {
            bool lateral = IsWord(tokens[position], "LATERAL");
            if (lateral) position++;
            if (position >= tokens.Count) return;
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
                else ReadGroupedSources(tokens, position, sourceNames, lateralSources);
                position = SkipParentheses(tokens, position);
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
                    position = SkipParentheses(tokens, position);
                }

                var alias = AliasAt(tokens, ref position);
                if (position + 1 < tokens.Count && IsWord(tokens[position], "WITH") && tokens[position + 1].Kind == SqlTokenKind.OpenParenthesis)
                {
                    // A table hint: WITH (NOLOCK).
                    position = SkipParentheses(tokens, position + 1);
                }

                scope.Sources.Add((derived ? null : table, alias ?? (derived ? table : null)));
                if (qualifiedTable != null)
                    (scope.QualifiedSources ??= new()).Add((qualifiedTable, alias, first));
            }

            if (!isFrom || position >= tokens.Count || tokens[position].Text != ",")
            {
                return;
            }

            position++;
        }
    }

    private void ReadGroupedSources(IReadOnlyList<SqlToken> tokens, int open, ISet<int>? sourceNames, ISet<int>? lateralSources)
    {
        // Parenthesized table factors and joins share their enclosing SELECT's relation scope.
        ReadSources(tokens, open, sourceNames, lateralSources);
        int end = SkipParentheses(tokens, open) - 1;
        for (int index = open + 1; index < end; index++)
        {
            if (tokens[index].Kind == SqlTokenKind.OpenParenthesis) index = SkipParentheses(tokens, index) - 1;
            else if (IsWord(tokens[index], "JOIN")) ReadSources(tokens, index, sourceNames, lateralSources);
        }
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

        if (position < tokens.Count && IsName(tokens[position]) &&
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

        /// <summary>Full relation identities, allocated only for the explicit-qualifier validation scan.</summary>
        internal List<(string[] Table, string? Alias, int Position)>? QualifiedSources { get; set; }
    }
}
