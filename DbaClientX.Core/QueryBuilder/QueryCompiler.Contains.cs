using System.Collections.Generic;
using System.Text;

namespace DBAClientX.QueryBuilder;

public partial class QueryCompiler
{
    /// <summary>The <c>LIKE</c> escape character; it has no special meaning in any dialect's string literals.</summary>
    private const char LikeEscape = '!';

    private void AppendContains(StringBuilder sb, ContainsToken token, List<object>? parameters)
    {
        var expression = token.IsRaw ? token.Expression : QuoteIdentifier(token.Expression);
        var value = ContainsParameterValue(token);
        if (_dialect == SqlDialect.SQLite)
        {
            // SQLite's LIKE ignores ASCII case whatever the collation, so case-sensitive matching needs instr(), which
            // compares exactly and needs no escaping. Folded matching uses instr() over lower() too: the rows of
            // LOWER(x) LIKE LOWER(p) for ordinary text (and correct for NULs and long text, where LIKE is not), in about
            // 40% less time on a large scan.
            sb.Append("instr(");
            if (token.CaseInsensitive)
            {
                sb.Append("lower(").Append(expression).Append("), lower(");
                AppendValue(sb, value, parameters);
                sb.Append(')');
            }
            else
            {
                sb.Append(expression).Append(", ");
                AppendValue(sb, value, parameters);
            }

            sb.Append(") > 0");
            return;
        }

        if (token.CaseInsensitive && _dialect == SqlDialect.PostgreSql)
        {
            sb.Append(expression).Append(" ILIKE ");
            AppendValue(sb, value, parameters);
        }
        else if (token.CaseInsensitive)
        {
            sb.Append("LOWER(").Append(expression).Append(") LIKE LOWER(");
            AppendValue(sb, value, parameters);
            sb.Append(')');
        }
        else
        {
            sb.Append(expression).Append(" LIKE ");
            AppendValue(sb, value, parameters);
        }

        sb.Append(" ESCAPE '").Append(LikeEscape).Append('\'');
    }

    /// <summary>The value bound for a contains condition: the escaped <c>LIKE</c> pattern, or the text for <c>instr()</c>.</summary>
    private string ContainsParameterValue(ContainsToken token)
        => _dialect == SqlDialect.SQLite
            ? token.Text
            : "%" + EscapeLikeText(token.Text) + "%";

    /// <summary>
    /// Escapes the characters <c>LIKE</c> treats as patterns: <c>%</c>, <c>_</c>, the escape character itself, and on
    /// SQL Server the <c>[</c> that opens a character class.
    /// </summary>
    private string EscapeLikeText(string text)
    {
        var builder = new StringBuilder(text.Length + 8);
        foreach (var character in text)
        {
            if (character == '%' || character == '_' || character == LikeEscape ||
                (character == '[' && _dialect == SqlDialect.SqlServer))
            {
                builder.Append(LikeEscape);
            }

            builder.Append(character);
        }

        return builder.ToString();
    }
}
