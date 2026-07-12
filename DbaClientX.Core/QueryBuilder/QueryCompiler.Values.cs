using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace DBAClientX.QueryBuilder;

public partial class QueryCompiler
{
    private void AppendValues(StringBuilder builder, IReadOnlyList<object> values, List<object>? parameters)
    {
        for (var index = 0; index < values.Count; index++)
        {
            if (index > 0)
            {
                builder.Append(", ");
            }

            AppendValue(builder, values[index], parameters);
        }
    }

    private void AppendValue(StringBuilder builder, object value, List<object>? parameters)
    {
        if (value is Query query)
        {
            builder.Append('(').Append(CompileInternal(query, parameters)).Append(')');
            return;
        }

        builder.Append(parameters != null ? AddParameter(value, parameters) : FormatValue(value));
    }

    private static string AddParameter(object value, List<object> parameters)
    {
        var name = "@p" + parameters.Count;
        parameters.Add(value);
        return name;
    }

    private string FormatValue(object value)
    {
        return value switch
        {
            string text => $"'{text.Replace("'", "''")}'",
            null => "NULL",
            bool boolean => FormatBooleanLiteral(boolean),
            DateTime dateTime => $"'{dateTime.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)}'",
            DateTimeOffset dateTimeOffset => $"'{dateTimeOffset.ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzz", CultureInfo.InvariantCulture)}'",
            decimal number => number.ToString(CultureInfo.InvariantCulture),
            double number => number.ToString(CultureInfo.InvariantCulture),
            float number => number.ToString(CultureInfo.InvariantCulture),
            _ => value.ToString() ?? string.Empty
        };
    }

    private string FormatBooleanLiteral(bool value)
        => _dialect switch
        {
            SqlDialect.PostgreSql => value ? "TRUE" : "FALSE",
            _ => value ? "1" : "0"
        };
}
