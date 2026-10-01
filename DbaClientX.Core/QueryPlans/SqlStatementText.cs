using System;
using System.Collections.Generic;
using DBAClientX.QueryBuilder;

namespace DBAClientX.QueryPlans;

/// <summary>Splits SQL text into statements, for plan tools that explain or check one statement at a time.</summary>
public static class SqlStatementText
{
    /// <summary>
    /// Returns whether <paramref name="sql"/> holds at most one SQLite statement (see <see cref="Split(string)"/>):
    /// comments and semicolons around it do not count.
    /// </summary>
    /// <param name="sql">The SQL text.</param>
    /// <returns><see langword="true"/> for a single statement or none.</returns>
    public static bool IsSingleStatement(string sql) => Split(sql).Count <= 1;

    /// <summary>Returns the statements of an SQLite script, in order (see <see cref="Split(string, SqlDialect)"/>).</summary>
    /// <param name="sql">The SQL text; statements are separated by semicolons.</param>
    /// <returns>The statements; empty when the text holds none.</returns>
    public static IReadOnlyList<string> Split(string sql) => Split(sql, SqlDialect.SQLite);

    /// <summary>
    /// Returns the statements of a script, in order: the text from each statement's first token to its last, without
    /// the semicolon that ends it. Comments between statements and empty statements are dropped; comments inside a
    /// statement are kept.
    /// </summary>
    /// <param name="sql">The SQL text; statements are separated by semicolons.</param>
    /// <param name="dialect">The dialect whose quoting and bodies apply.</param>
    /// <returns>The statements; empty when the text holds none.</returns>
    /// <remarks>
    /// A semicolon ends a statement unless it is inside a string, a quoted or bracketed identifier or a comment, and
    /// for <see cref="SqlDialect.SQLite"/> the <c>BEGIN … END</c> body of a <c>CREATE TRIGGER</c>, and for
    /// <see cref="SqlDialect.PostgreSql"/> a dollar-quoted body (<c>$$ … $$</c>, <c>$fn$ … $fn$</c>; in SQLite these are
    /// parameter names). Procedural bodies of other dialects (MySQL and T-SQL <c>BEGIN … END</c>), PostgreSQL
    /// <c>BEGIN ATOMIC</c> bodies and scripts that rely on line breaks or batch separators instead of semicolons
    /// (<c>GO</c>) are not kept whole.
    /// </remarks>
    public static IReadOnlyList<string> Split(string sql, SqlDialect dialect)
    {
        if (sql == null)
        {
            throw new ArgumentNullException(nameof(sql));
        }

        var statements = new List<string>();
        var tokens = SqlTokenizer.Tokenize(sql, dollarQuotes: dialect == SqlDialect.PostgreSql);
        var first = -1;
        var trigger = false;
        var inBody = false;
        for (var index = 0; index < tokens.Count; index++)
        {
            var token = tokens[index];
            if (token.Kind == SqlTokenKind.Semicolon && !inBody)
            {
                Add(statements, sql, tokens, first, index - 1);
                first = -1;
                trigger = false;
                continue;
            }

            if (first < 0)
            {
                first = index;
                trigger = dialect == SqlDialect.SQLite && IsWord(token, "CREATE") && IsTrigger(tokens, index + 1);
            }

            if (!trigger)
            {
                continue;
            }

            if (!inBody)
            {
                inBody = IsWord(token, "BEGIN");
            }
            else if (IsWord(token, "END") && tokens[index - 1].Kind == SqlTokenKind.Semicolon)
            {
                // Every statement of an SQLite trigger body ends with a semicolon, so the body's END follows one; the
                // END of a CASE (or a column named "end") never does.
                inBody = false;
            }
        }

        Add(statements, sql, tokens, first, tokens.Count - 1);
        return statements;
    }

    /// <summary>Whether <c>CREATE</c> is followed by <c>[TEMP|TEMPORARY] TRIGGER</c>.</summary>
    private static bool IsTrigger(IReadOnlyList<SqlToken> tokens, int index)
    {
        if (index < tokens.Count && (IsWord(tokens[index], "TEMP") || IsWord(tokens[index], "TEMPORARY")))
        {
            index++;
        }

        return index < tokens.Count && IsWord(tokens[index], "TRIGGER");
    }

    private static void Add(List<string> statements, string sql, IReadOnlyList<SqlToken> tokens, int first, int last)
    {
        if (first < 0 || last < first)
        {
            return;
        }

        var start = tokens[first].Position;
        var end = tokens[last].Position + tokens[last].Text.Length;
        statements.Add(sql.Substring(start, end - start));
    }

    private static bool IsWord(SqlToken token, string word)
        => token.Kind == SqlTokenKind.Word && string.Equals(token.Text, word, StringComparison.OrdinalIgnoreCase);
}
