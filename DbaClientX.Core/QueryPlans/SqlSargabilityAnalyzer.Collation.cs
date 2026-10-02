using System;
using System.Collections.Generic;

namespace DBAClientX.QueryPlans;

public static partial class SqlSargabilityAnalyzer
{
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

    private static bool IsValue(SqlToken token) => token.Kind is SqlTokenKind.Parameter or SqlTokenKind.String or SqlTokenKind.Number;

    private static bool IsComparison(SqlToken token)
        => (token.Kind == SqlTokenKind.Symbol && token.Text is "=" or "<" or ">" or "!") ||
           IsWord(token, "IS") || IsWord(token, "NOT") || IsWord(token, "LIKE") || IsWord(token, "GLOB");
}
