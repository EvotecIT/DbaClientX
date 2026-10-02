using System;
using System.Collections.Generic;
using System.Text;
using DBAClientX.QueryPlans;

namespace DBAClientX.QueryBuilder;

public partial class QueryCompiler
{
    private static readonly HashSet<string> BareProjectionExpressions = new(StringComparer.OrdinalIgnoreCase)
    {
        "NULL", "TRUE", "FALSE", "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME",
        "CURRENT_USER", "SESSION_USER", "SYSTEM_USER", "USER", "LOCALTIME", "LOCALTIMESTAMP"
    };

    private static readonly HashSet<string> ProjectionOperators = new(StringComparer.OrdinalIgnoreCase)
    {
        "AND", "OR", "NOT", "IS", "LIKE", "IN", "BETWEEN", "COLLATE", "WHEN", "THEN", "ELSE", "AS"
    };

    private static readonly HashSet<string> MySqlProjectionOperators = new(StringComparer.OrdinalIgnoreCase)
    {
        "XOR", "REGEXP", "RLIKE", "DIV", "MOD", "BINARY"
    };

    private static readonly HashSet<string> IntervalUnits = new(StringComparer.OrdinalIgnoreCase)
    {
        "MICROSECOND", "SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR",
        "SECOND_MICROSECOND", "MINUTE_MICROSECOND", "MINUTE_SECOND", "HOUR_MICROSECOND", "HOUR_SECOND",
        "HOUR_MINUTE", "DAY_MICROSECOND", "DAY_SECOND", "DAY_MINUTE", "DAY_HOUR", "YEAR_MONTH"
    };

    private void AppendDerivedSelect(StringBuilder sb, string sql, string alias, Query query, int? top = null)
    {
        // SQL Server requires named derived columns; it and MySQL require unique names. Naming
        // columns by ordinal preserves positional set semantics without rewriting trusted expressions.
        var names = _dialect is SqlDialect.SqlServer or SqlDialect.MySql ? GetProjectionNames(query) : null;
        bool rename = names != null && RequiresDerivedNames(names);
        sb.Append("SELECT ");
        if (top.HasValue) sb.Append("TOP ").Append(top.Value).Append(' ');
        if (!rename) sb.Append('*');
        else
        {
            var outputNames = GetDerivedOutputNames(names!);
            for (int index = 0; index < names!.Count; index++)
            {
                if (index > 0) sb.Append(", ");
                string internalName = "dbx_column_" + index;
                sb.Append(SqlIdentifier.Quote(_dialect, internalName));
                if (!string.Equals(internalName, outputNames[index], StringComparison.Ordinal))
                    sb.Append(" AS ").Append(SqlIdentifier.Quote(_dialect, outputNames[index]));
            }
        }
        if (rename && _dialect == SqlDialect.MySql)
            AppendMySqlNamedSource(sb, sql, alias, names!.Count);
        else
            sb.Append(" FROM (").Append(sql).Append(')');
        AppendAlias(sb, alias);
        if (rename && _dialect != SqlDialect.MySql)
        {
            sb.Append(" (");
            for (int index = 0; index < names!.Count; index++)
            {
                if (index > 0) sb.Append(", ");
                sb.Append(SqlIdentifier.Quote(_dialect, "dbx_column_" + index));
            }
            sb.Append(')');
        }
    }

    private static IReadOnlyList<string> GetDerivedOutputNames(IReadOnlyList<string?> names)
    {
        var used = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var name in names) if (name != null) used.Add(name);
        var result = new string[names.Count];
        for (int index = 0; index < names.Count; index++)
        {
            if (names[index] is { } explicitName) result[index] = explicitName;
            else
            {
                string baseName = "dbx_column_" + index;
                string name = baseName;
                for (int suffix = 1; !used.Add(name); suffix++) name = baseName + "_" + suffix;
                result[index] = name;
            }
        }
        return result;
    }

    private void AppendMySqlNamedSource(StringBuilder sb, string sql, string alias, int columnCount)
    {
        // Correlation column lists require MariaDB 11.7. A scoped CTE works with MySQL 8 and MariaDB 10.2,
        // preserves local ordering/paging and does not rewrite trusted projection expressions.
        string baseName = alias + "_source";
        string name = baseName;
        for (int suffix = 1; sql.IndexOf(name, StringComparison.OrdinalIgnoreCase) >= 0; suffix++)
            name = baseName + "_" + suffix;
        string quotedName = SqlIdentifier.Quote(_dialect, name);
        sb.Append(" FROM (WITH ").Append(quotedName).Append(" (");
        for (int index = 0; index < columnCount; index++)
        {
            if (index > 0) sb.Append(", ");
            sb.Append(SqlIdentifier.Quote(_dialect, "dbx_column_" + index));
        }
        sb.Append(") AS (").Append(sql).Append(") SELECT * FROM ").Append(quotedName).Append(')');
    }

    private static bool RequiresDerivedNames(IReadOnlyList<string?> names)
    {
        var unique = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var name in names)
            if (name == null || !unique.Add(name)) return true;
        return false;
    }

    // Wildcards have an unknown width without database metadata. Retain them as SQL, where the provider
    // validates names. Explicit projections can be named by ordinal, including raw lists of expressions.
    private IReadOnlyList<string?>? GetProjectionNames(Query query)
    {
        if (query.SelectExpressions.Count == 0) return null;
        var names = new List<string?>();
        bool standalone = query.TableExpression == null && !query.FromSubquery.HasValue;
        foreach (var expression in query.SelectExpressions)
        {
            if (!expression.IsRaw)
            {
                if (expression.Text == "*" || expression.Text.EndsWith(".*", StringComparison.Ordinal)) return null;
                names.Add(standalone && LooksLikeStandaloneLiteral(expression.Text)
                    ? null : expression.Text.Substring(expression.Text.LastIndexOf('.') + 1));
                continue;
            }

            var tokens = SqlTokenizer.Tokenize(expression.Text, backslashStrings: _dialect == SqlDialect.MySql);
            int first = 0, depth = 0;
            for (int index = 0; index <= tokens.Count; index++)
            {
                if (index < tokens.Count)
                {
                    if (tokens[index].Kind == SqlTokenKind.OpenParenthesis) depth++;
                    else if (tokens[index].Kind == SqlTokenKind.CloseParenthesis) depth--;
                    if (depth != 0 || tokens[index].Text != ",") continue;
                }
                if (IsWildcard(tokens, first, index)) return null;
                names.Add(GetRawProjectionName(tokens, first, index));
                first = index + 1;
            }
        }
        return names;
    }

    private static bool IsWildcard(IReadOnlyList<SqlToken> tokens, int first, int end)
        => end > first && tokens[end - 1].Text == "*" &&
           (end - first == 1 || end - first >= 3 && tokens[end - 2].Text == ".");

    private string? GetRawProjectionName(IReadOnlyList<SqlToken> tokens, int first, int end)
    {
        if (end <= first) return null;
        var last = tokens[end - 1];
        bool name = last.Kind is SqlTokenKind.Word or SqlTokenKind.QuotedIdentifier or SqlTokenKind.String;
        if (_dialect == SqlDialect.SqlServer && end - first >= 3 && tokens[first + 1].Text == "=" &&
            tokens[first].Kind is SqlTokenKind.Word or SqlTokenKind.QuotedIdentifier or SqlTokenKind.String)
            return tokens[first].Value;
        if (end - first >= 3 && string.Equals(tokens[end - 2].Text, "AS", StringComparison.OrdinalIgnoreCase))
            return last.Value.Length > 0 ? last.Value : null;
        if (_dialect == SqlDialect.MySql && IsMySqlExpressionTail(tokens, first, end)) return null;
        // Plain identifiers, including a table qualifier, carry the final identifier's name.
        bool identifier = last.Kind is SqlTokenKind.Word or SqlTokenKind.QuotedIdentifier;
        for (int index = first; index < end && identifier; index++)
            identifier = (index - first) % 2 == 0
                ? tokens[index].Kind is SqlTokenKind.Word or SqlTokenKind.QuotedIdentifier
                : tokens[index].Text == ".";
        if (identifier && !(end - first == 1 && last.Kind == SqlTokenKind.Word && BareProjectionExpressions.Contains(last.Text)))
            return last.Value;
        // An implicit alias follows an expression's final value, not a SQL operator or CASE's END.
        if (name && !(last.Kind == SqlTokenKind.Word &&
                      (BareProjectionExpressions.Contains(last.Text) || last.Text.Equals("END", StringComparison.OrdinalIgnoreCase))) &&
            end - first >= 2)
        {
            var previous = tokens[end - 2];
            if (previous.Kind is SqlTokenKind.CloseParenthesis or SqlTokenKind.String or SqlTokenKind.Number or SqlTokenKind.QuotedIdentifier ||
                previous.Kind == SqlTokenKind.Word && !ProjectionOperators.Contains(previous.Text)
                && !(_dialect == SqlDialect.MySql && MySqlProjectionOperators.Contains(previous.Text))
                && !(_dialect == SqlDialect.SqlServer && previous.Text.Equals("ZONE", StringComparison.OrdinalIgnoreCase))) return last.Value;
        }
        return null;
    }

    private static bool IsMySqlExpressionTail(IReadOnlyList<SqlToken> tokens, int first, int end)
    {
        var last = tokens[end - 1];
        if (end - first >= 2 && last.Kind == SqlTokenKind.String && tokens[end - 2].Kind == SqlTokenKind.Word)
        {
            string prefix = tokens[end - 2].Text;
            if (prefix.StartsWith("_", StringComparison.Ordinal) || prefix.Equals("X", StringComparison.OrdinalIgnoreCase)
                || prefix.Equals("B", StringComparison.OrdinalIgnoreCase) || prefix.Equals("DATE", StringComparison.OrdinalIgnoreCase)
                || prefix.Equals("TIME", StringComparison.OrdinalIgnoreCase) || prefix.Equals("TIMESTAMP", StringComparison.OrdinalIgnoreCase)) return true;
        }
        if (last.Kind != SqlTokenKind.Word || !IntervalUnits.Contains(last.Text)) return false;
        int depth = 0, interval = -1;
        for (int index = first; index < end - 1; index++)
        {
            if (tokens[index].Kind == SqlTokenKind.OpenParenthesis) depth++;
            else if (tokens[index].Kind == SqlTokenKind.CloseParenthesis) depth--;
            else if (depth == 0 && tokens[index].Kind == SqlTokenKind.Word
                     && tokens[index].Text.Equals("INTERVAL", StringComparison.OrdinalIgnoreCase)) interval = index;
        }
        // A trailing unit belongs to INTERVAL's expression. An alias may itself be a unit word,
        // but then the expression already ends with its own unit (INTERVAL 1 DAY DAY).
        return interval >= 0 && !(end - 2 > interval + 1 && tokens[end - 2].Kind == SqlTokenKind.Word
            && IntervalUnits.Contains(tokens[end - 2].Text));
    }
}
