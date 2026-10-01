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

    /// <summary>
    /// Appends <c> COLLATE name</c>. The name holds only ASCII letters, digits and underscores (checked again here, so no
    /// text reaches the SQL unchecked); PostgreSQL quotes it because its collation names keep their case.
    /// </summary>
    private void AppendCollation(StringBuilder builder, string collation)
    {
        foreach (var character in collation)
        {
            if (!(character is >= 'a' and <= 'z' or >= 'A' and <= 'Z' or >= '0' and <= '9' or '_'))
            {
                throw new ArgumentException($"Collation '{collation}' is not a plain collation name.", nameof(collation));
            }
        }

        builder.Append(" COLLATE ").Append(_dialect == SqlDialect.PostgreSql ? SqlIdentifier.Quote(_dialect, collation) : collation);
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
