using System;

namespace DBAClientX.QueryPlans;

/// <summary>Checks on SQL text that plan tools need before they prefix or wrap a statement.</summary>
public static class SqlStatementText
{
    /// <summary>
    /// Returns whether <paramref name="sql"/> holds at most one statement: nothing but comments and semicolons follows
    /// the first top-level semicolon. Semicolons in strings, quoted identifiers and comments do not count.
    /// </summary>
    /// <param name="sql">The SQL text.</param>
    /// <returns><see langword="true"/> for a single statement.</returns>
    public static bool IsSingleStatement(string sql)
    {
        if (sql == null)
        {
            throw new ArgumentNullException(nameof(sql));
        }

        var ended = false;
        foreach (var token in SqlTokenizer.Tokenize(sql))
        {
            if (token.Kind == SqlTokenKind.Semicolon)
            {
                ended = true;
            }
            else if (ended)
            {
                return false;
            }
        }

        return true;
    }
}
