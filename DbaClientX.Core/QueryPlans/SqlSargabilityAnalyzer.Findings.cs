using System;
using System.Collections.Generic;

namespace DBAClientX.QueryPlans;

public static partial class SqlSargabilityAnalyzer
{
    /// <summary>A function around a column, from the function name at <paramref name="function"/> to its closing parenthesis.</summary>
    private static SqlSargabilityFinding FunctionFinding(string sql, IReadOnlyList<SqlToken> tokens, int function, int column, SqlSourceScopes scopes)
    {
        var token = tokens[function];
        var end = FindClosingParenthesis(tokens, function + 1);
        var text = end < 0 ? sql.Substring(token.Position) : sql.Substring(token.Position, tokens[end].Position + 1 - token.Position);
        var (name, table) = ColumnAndTable(tokens, column, scopes);
        return new SqlSargabilityFinding(
            SqlSargabilityFindingKind.FunctionOnColumn,
            token.Position,
            text,
            $"{token.Text.ToUpperInvariant()}() wraps a column{Describe(name, table)} in a condition, so an index on that column cannot serve it; compare the stored value, store a normalized column, or index the expression.",
            name,
            table);
    }

    /// <summary>A PostgreSQL <c>::</c> cast of the column at <paramref name="index"/>.</summary>
    private static SqlSargabilityFinding CastFinding(string sql, IReadOnlyList<SqlToken> tokens, int index, SqlSourceScopes scopes)
    {
        var end = Math.Min(index + 2, tokens.Count - 1);
        var first = QualifiedStart(tokens, index);
        var (name, table) = ColumnAndTable(tokens, index, scopes);
        return new SqlSargabilityFinding(
            SqlSargabilityFindingKind.FunctionOnColumn,
            tokens[first].Position,
            sql.Substring(tokens[first].Position, tokens[end].Position + tokens[end].Text.Length - tokens[first].Position),
            $"A :: cast converts a column{Describe(name, table)} in a condition, so an index on that column cannot serve it; cast the compared value instead.",
            name,
            table);
    }

    /// <summary>A <c>LIKE</c>, <c>ILIKE</c> or <c>GLOB</c> at <paramref name="index"/> whose literal pattern starts with a wildcard.</summary>
    private static SqlSargabilityFinding LeadingWildcardFinding(string sql, IReadOnlyList<SqlToken> tokens, int index, SqlSourceScopes scopes)
    {
        var token = tokens[index];
        var pattern = tokens[index + 1];
        var subject = index > 0 && IsWord(tokens[index - 1], "NOT") ? index - 2 : index - 1;
        var (name, table) = subject >= 0 && IsColumnReference(tokens, subject) ? ColumnAndTable(tokens, subject, scopes) : (null, null);
        return new SqlSargabilityFinding(
            SqlSargabilityFindingKind.LeadingWildcard,
            token.Position,
            sql.Substring(token.Position, pattern.Position + pattern.Text.Length - token.Position),
            $"{token.Text.ToUpperInvariant()} pattern starts with a wildcard{Describe(name, table)}, so an index cannot narrow it and every row is read; use a prefix pattern or a full-text index.",
            name,
            table);
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

    private static bool StartsWithWildcard(string pattern, bool glob)
        => pattern.Length > 0 && (glob ? pattern[0] is '*' or '?' or '[' : pattern[0] is '%' or '_');
}
