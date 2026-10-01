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

        // Identifier APIs of the query builder treat dots as schema separators; each part is quoted by SqlIdentifier.
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

            if (parts[index].Length == 0)
            {
                throw new ArgumentException($"Identifier '{identifier}' has an empty dotted part.", nameof(identifier));
            }

            builder.Append(SqlIdentifier.Quote(_dialect, parts[index]));
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
