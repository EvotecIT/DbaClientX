using System;
using System.Collections.Generic;
using System.Globalization;
using DBAClientX.QueryPlans;

namespace DBAClientX.QueryBuilder;

public partial class QueryCompiler
{
    private string CompileCompoundOrderColumn(Query query, string column, string? collation)
    {
        if (column.IndexOf('.') < 0) return QuoteIdentifier(column);

        // Source qualifiers do not survive generated compound wrappers. Known projection ordinals
        // also avoid duplicate names and provider-specific folding of raw aliases.
        var names = GetProjectionNames(query);
        string[] parts = column.Split('.');
        int ordinal = 0;
        foreach (var expression in query.SelectExpressions)
        {
            if (!expression.IsRaw)
            {
                if (string.Equals(expression.Text, column, StringComparison.Ordinal))
                    return CompileProjectedOrder(parts[parts.Length - 1], ordinal, names, collation);
                ordinal++;
                continue;
            }
            var tokens = SqlTokenizer.Tokenize(expression.Text, out _, backslashStrings: _dialect == SqlDialect.MySql,
                dollarQuotes: _dialect == SqlDialect.PostgreSql,
                nestedBlockComments: _dialect is SqlDialect.SqlServer or SqlDialect.PostgreSql,
                bracketIdentifiers: _dialect != SqlDialect.PostgreSql, oracleAlternativeQuotes: _dialect == SqlDialect.Oracle);
            int first = 0, depth = 0;
            for (int end = 0; end <= tokens.Count; end++)
            {
                if (end < tokens.Count)
                {
                    if (!IsProjectionSeparator(tokens[end], ref depth)) continue;
                }
                string? name = GetRawProjectionName(tokens, first, end);
                if (name != null && IsDirectOrderedColumn(tokens, first, end, parts))
                {
                    var nameToken = _dialect == SqlDialect.SqlServer && first + 1 < end && tokens[first + 1].Text == "="
                        ? tokens[first] : tokens[end - 1];
                    return CompileProjectedOrder(name, ordinal, names, collation, nameToken.Kind == SqlTokenKind.Word);
                }
                ordinal++;
                first = end + 1;
            }
        }
        // Wildcard width is provider-owned. Its qualified output still loses the source qualifier.
        if (names == null) return SqlIdentifier.Quote(_dialect, parts[parts.Length - 1]);
        throw new InvalidOperationException($"Compound ordering column '{column}' is not directly projected. Order by its output alias or use an output ordinal with OrderByRaw.");
    }

    private string CompileProjectedOrder(string name, int ordinal, IReadOnlyList<string?>? names, string? collation, bool unquoted = false)
    {
        if (names != null && collation == null) return (ordinal + 1).ToString(CultureInfo.InvariantCulture);
        if (names != null)
        {
            int matches = 0;
            foreach (var output in names) if (string.Equals(output, name, StringComparison.OrdinalIgnoreCase)) matches++;
            if (matches > 1)
            {
                if (collation != null)
                    throw new InvalidOperationException("Collated compound ordering requires a unique projected output alias.");
                return (ordinal + 1).ToString(CultureInfo.InvariantCulture);
            }
        }
        // With provider-owned wildcard width, retain the spelling/quoting of raw output names.
        // Emitting a parsed word unquoted preserves PostgreSQL/Oracle alias folding.
        return unquoted ? name : SqlIdentifier.Quote(_dialect, name);
    }

    private bool IsDirectOrderedColumn(IReadOnlyList<SqlToken> tokens, int first, int end, string[] parts)
    {
        if (_dialect == SqlDialect.SqlServer && first + 1 < end && tokens[first + 1].Text == "=") first += 2;
        int index = first;
        for (int part = 0; part < parts.Length; part++)
        {
            if (index >= end || tokens[index].Kind is not (SqlTokenKind.Word or SqlTokenKind.QuotedIdentifier)
                || !string.Equals(tokens[index++].Value, parts[part], StringComparison.Ordinal)) return false;
            if (part + 1 < parts.Length && (index >= end || tokens[index++].Text != ".")) return false;
        }
        return index == end || index + 1 == end
            || index + 2 == end && tokens[index].Text.Equals("AS", StringComparison.OrdinalIgnoreCase);
    }
}
