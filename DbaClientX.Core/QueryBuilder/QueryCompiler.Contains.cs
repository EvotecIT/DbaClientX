using System;
using System.Collections.Generic;
using System.Text;

namespace DBAClientX.QueryBuilder;

public partial class QueryCompiler
{
    /// <summary>The <c>LIKE</c> escape character; it has no special meaning in any dialect's string literals.</summary>
    private const char LikeEscape = '!';

    /// <summary>
    /// The SQLite function that folds with .NET's invariant rules; DbaClientX.SQLite registers it under this name
    /// (<c>SQLiteUnicodeText.LowerFunction</c>).
    /// </summary>
    private const string SQLiteInvariantLowerFunction = "dbx_lower";

    private void AppendContains(StringBuilder sb, ContainsToken token, List<object>? parameters)
    {
        var expression = token.IsRaw ? token.Expression : QuoteIdentifier(token.Expression);
        var value = ContainsParameterValue(token);
        if (_dialect == SqlDialect.SQLite)
        {
            // SQLite's LIKE ignores ASCII case whatever the collation, stops at a NUL and limits the pattern length, so
            // every folding uses instr(), which compares exactly and needs no escaping. Measured on 1,000,000 rows it was
            // also as fast as or faster than an escaped GLOB '*text*' in every case.
            sb.Append("instr(");
            switch (token.Folding)
            {
                case TextFolding.Database:
                    sb.Append("lower(").Append(expression).Append("), lower(");
                    AppendValue(sb, value, parameters);
                    sb.Append(')');
                    break;
                case TextFolding.Invariant:
                    // The text is folded in .NET (ContainsParameterValue), the column by the registered function.
                    sb.Append(SQLiteInvariantLowerFunction).Append('(').Append(expression).Append("), ");
                    AppendValue(sb, value, parameters);
                    break;
                default:
                    sb.Append(expression).Append(", ");
                    AppendValue(sb, value, parameters);
                    break;
            }

            sb.Append(") > 0");
            return;
        }

        if (token.Folding == TextFolding.Invariant)
        {
            throw new NotSupportedException(
                $"TextFolding.Invariant is available on SQLite only (with SQLiteUnicodeText registered); {_dialect}'s LOWER folds Unicode letters under a Unicode-aware collation or locale, so use TextFolding.Database.");
        }

        if (token.Folding == TextFolding.Database && _dialect == SqlDialect.PostgreSql)
        {
            sb.Append(expression).Append(" ILIKE ");
            AppendValue(sb, value, parameters);
        }
        else if (token.Folding == TextFolding.Database)
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

    /// <summary>
    /// The value bound for a contains condition: the escaped <c>LIKE</c> pattern, or the text for <c>instr()</c>
    /// (lower-cased in .NET for <see cref="TextFolding.Invariant"/>).
    /// </summary>
    private string ContainsParameterValue(ContainsToken token)
        => _dialect == SqlDialect.SQLite
            ? token.Folding == TextFolding.Invariant ? token.Text.ToLowerInvariant() : token.Text
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
