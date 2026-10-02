using System;
using System.Collections.Generic;
using System.Linq;

namespace DBAClientX.QueryPlans;

/// <summary>
/// Finds the table aliases a statement defines (<c>FROM Probes p</c>, <c>JOIN Agents AS a</c>, <c>FROM a x, b y</c>,
/// <c>UPDATE Probes AS p</c>), so plan steps that a database reports by alias can be matched to table names. A
/// heuristic over tokens, not a parser: an alias defined for two different tables is reported with every candidate,
/// so rules apply when any of them is large.
/// </summary>
internal static class SqlTableAliases
{
    private static readonly HashSet<string> NotAliases = new(SqliteIdentifierComparer.Instance)
    {
        "WHERE", "JOIN", "LEFT", "RIGHT", "FULL", "INNER", "OUTER", "CROSS", "NATURAL", "ON", "USING", "GROUP", "ORDER",
        "LIMIT", "OFFSET", "UNION", "INTERSECT", "EXCEPT", "WINDOW", "HAVING", "SET", "VALUES", "RETURNING", "INDEXED",
        "NOT", "AS", "SELECT", "FROM", "WITH", "FETCH", "FOR", "OPTION", "PIVOT", "UNPIVOT", "LATERAL", "DEFAULT", "OR"
    };

    /// <summary>Returns the aliases with one table, and the aliases defined for several tables with all of them.</summary>
    internal static (IReadOnlyDictionary<string, string> Resolved, IReadOnlyDictionary<string, IReadOnlyList<string>> Ambiguous) Find(string sql)
        => Find(sql, includeSourceNames: false);

    internal static (IReadOnlyDictionary<string, string> Resolved, IReadOnlyDictionary<string, IReadOnlyList<string>> Ambiguous) FindSourceNames(string sql)
        => Find(sql, includeSourceNames: true);

    private static (IReadOnlyDictionary<string, string> Resolved, IReadOnlyDictionary<string, IReadOnlyList<string>> Ambiguous) Find(string sql, bool includeSourceNames)
    {
        var aliases = new Dictionary<string, string>(SqliteIdentifierComparer.Instance);
        var ambiguous = new Dictionary<string, List<string>>(SqliteIdentifierComparer.Instance);
        var tokens = SqlTokenizer.Tokenize(sql);
        for (var index = 0; index < tokens.Count; index++)
        {
            var token = tokens[index];
            var isFrom = Is(token, "FROM");
            if (!isFrom && !Is(token, "JOIN") && !Is(token, "UPDATE") && !Is(token, "INTO"))
            {
                continue;
            }

            // FROM lists continue after commas at the same nesting level.
            var position = index + 1;
            while (true)
            {
                position = ReadTableReference(tokens, position, aliases, ambiguous, includeSourceNames);
                if (!isFrom || position >= tokens.Count || tokens[position].Text != ",")
                {
                    break;
                }

                position++;
            }
        }

        foreach (var alias in ambiguous.Keys)
        {
            aliases.Remove(alias);
        }

        var candidates = new Dictionary<string, IReadOnlyList<string>>(SqliteIdentifierComparer.Instance);
        foreach (var pair in ambiguous)
        {
            candidates[pair.Key] = pair.Value;
        }

        return (aliases, candidates);
    }

    /// <summary>Reads <c>[schema.]table [[AS] alias]</c> at <paramref name="position"/> and returns the position after it.</summary>
    private static int ReadTableReference(IReadOnlyList<SqlToken> tokens, int position, Dictionary<string, string> aliases, Dictionary<string, List<string>> ambiguous, bool includeSourceNames)
    {
        if (position >= tokens.Count)
        {
            return position;
        }

        if (tokens[position].Kind == SqlTokenKind.OpenParenthesis)
        {
            // A subquery source: skip it; an alias after it names no table.
            var depth = 0;
            for (; position < tokens.Count; position++)
            {
                if (tokens[position].Kind == SqlTokenKind.OpenParenthesis)
                {
                    depth++;
                }
                else if (tokens[position].Kind == SqlTokenKind.CloseParenthesis && --depth == 0)
                {
                    position++;
                    break;
                }
            }

            return SkipAlias(tokens, position);
        }

        if (!IsName(tokens[position]) || IsKeyword(tokens[position]))
        {
            return position;
        }

        var table = tokens[position].Value;
        var displayedName = table;
        while (position + 2 < tokens.Count && tokens[position + 1].Text == "." && IsName(tokens[position + 2]))
        {
            position += 2;
            table = tokens[position].Value;
            displayedName += "." + table;
        }

        if (includeSourceNames) AddName(displayedName, table, aliases, ambiguous);

        position++;
        if (position < tokens.Count && Is(tokens[position], "AS"))
        {
            position++;
        }

        if (position < tokens.Count && IsName(tokens[position]) && !IsKeyword(tokens[position]))
        {
            var alias = tokens[position].Value;
            AddName(alias, table, aliases, ambiguous);
            position++;
        }

        return position;
    }

    private static void AddName(string alias, string table, Dictionary<string, string> aliases, Dictionary<string, List<string>> ambiguous)
    {
        if (ambiguous.TryGetValue(alias, out var tables))
        {
            if (!tables.Contains(table, SqliteIdentifierComparer.Instance))
            {
                tables.Add(table);
            }
        }
        else if (aliases.TryGetValue(alias, out var known) && !SqliteIdentifierComparer.Instance.Equals(known, table))
        {
            ambiguous[alias] = new List<string> { known, table };
        }
        else
        {
            aliases[alias] = table;
        }

    }

    private static int SkipAlias(IReadOnlyList<SqlToken> tokens, int position)
    {
        if (position < tokens.Count && Is(tokens[position], "AS"))
        {
            position++;
        }

        return position < tokens.Count && IsName(tokens[position]) && !IsKeyword(tokens[position]) ? position + 1 : position;
    }

    private static bool Is(SqlToken token, string word)
        => token.Kind == SqlTokenKind.Word && SqliteIdentifierComparer.Instance.Equals(token.Text, word);

    private static bool IsName(SqlToken token)
        => token.Kind is SqlTokenKind.Word or SqlTokenKind.QuotedIdentifier;

    private static bool IsKeyword(SqlToken token)
        => token.Kind == SqlTokenKind.Word && NotAliases.Contains(token.Value);
}
