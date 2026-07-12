using System;
using System.Globalization;
using System.Text;

namespace DBAClientX.QueryBuilder;

public partial class QueryCompiler
{
    private void AppendAlias(StringBuilder builder, string alias)
    {
        builder.Append(_dialect == SqlDialect.Oracle ? " " : " AS ")
            .Append(QuoteIdentifier(alias));
    }

    private string QuoteIdentifier(string identifier)
    {
        if (string.IsNullOrEmpty(identifier) || identifier == "*")
        {
            return identifier;
        }

        var (open, close) = _dialect switch
        {
            SqlDialect.SqlServer => ('[', ']'),
            SqlDialect.MySql => ('`', '`'),
            SqlDialect.PostgreSql => ('"', '"'),
            SqlDialect.SQLite => ('"', '"'),
            SqlDialect.Oracle => ('"', '"'),
            _ => ('"', '"')
        };

        var parts = identifier.Split('.');
        var builder = new StringBuilder();
        for (var index = 0; index < parts.Length; index++)
        {
            if (index > 0)
            {
                builder.Append('.');
            }

            if (parts[index] == "*")
            {
                builder.Append('*');
                continue;
            }

            var escapedPart = parts[index].Replace(close.ToString(), new string(close, 2));
            builder.Append(open).Append(escapedPart).Append(close);
        }

        return builder.ToString();
    }

    private string QuoteSelectColumn(string identifier, bool allowStandaloneLiterals)
    {
        if (allowStandaloneLiterals && LooksLikeStandaloneLiteral(identifier))
        {
            return identifier;
        }

        return QuoteIdentifier(identifier);
    }

    private static bool LooksLikeStandaloneLiteral(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
        {
            return false;
        }

        if (double.TryParse(identifier, NumberStyles.Float, CultureInfo.InvariantCulture, out _))
        {
            return true;
        }

        return string.Equals(identifier, "NULL", StringComparison.OrdinalIgnoreCase)
            || string.Equals(identifier, "TRUE", StringComparison.OrdinalIgnoreCase)
            || string.Equals(identifier, "FALSE", StringComparison.OrdinalIgnoreCase);
    }
}
