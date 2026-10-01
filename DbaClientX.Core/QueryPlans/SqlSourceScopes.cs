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

    /// <summary>Enters a parenthesis: a subquery starts a level of its own, other parentheses stay in the current one.</summary>
    internal void Open(bool subquery) => _scopes.Push(subquery ? new Scope(_scopes.Peek()) : _scopes.Peek());

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
        _scopes.Push(new Scope(current.Parent));
    }

    /// <summary>
    /// Records the names a <c>WITH</c> at <paramref name="with"/> defines (<c>WITH [RECURSIVE] a [(cols)] AS (…), b AS (…)</c>),
    /// whose columns belong to no stored table. A <c>WITH (NOLOCK)</c> table hint defines none.
    /// </summary>
    internal void ReadCommonTableExpressions(IReadOnlyList<SqlToken> tokens, int with)
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
    internal void ReadSources(IReadOnlyList<SqlToken> tokens, int keyword)
    {
        var scope = _scopes.Peek();
        var isFrom = IsWord(tokens[keyword], "FROM");
        var position = keyword + 1;
        while (position < tokens.Count)
        {
            if (tokens[position].Kind == SqlTokenKind.OpenParenthesis)
            {
                // A derived table: its alias names no stored table.
                position = SkipParentheses(tokens, position);
                scope.Sources.Add((null, AliasAt(tokens, ref position)));
            }
            else if (IsName(tokens[position]) && !NotNames.Contains(tokens[position].Value))
            {
                var table = tokens[position].Value;
                while (position + 2 < tokens.Count && tokens[position + 1].Text == "." && IsName(tokens[position + 2]))
                {
                    position += 2;
                    table = tokens[position].Value;
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
            }

            if (!isFrom || position >= tokens.Count || tokens[position].Text != ",")
            {
                return;
            }

            position++;
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

        if (position < tokens.Count && IsName(tokens[position]) && !NotNames.Contains(tokens[position].Value))
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
        internal Scope(Scope? parent) => Parent = parent;

        internal Scope? Parent { get; }

        /// <summary>The sources of the level: a stored table (or null for a derived one) and its alias.</summary>
        internal List<(string? Table, string? Alias)> Sources { get; } = new();
    }
}
