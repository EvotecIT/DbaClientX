using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;

namespace DBAClientX.QueryBuilder;

internal sealed record QueryParameterReference(int Index);

public partial class QueryCompiler
{
    private void CollectParameters(Query query, List<object> parameters)
    {
        if (!string.IsNullOrWhiteSpace(query.InsertTable))
        {
            if (query.IsUpsert)
            {
                var row = query.InsertValues[0];
                CollectValues(row, parameters);
                if (_dialect == SqlDialect.SqlServer)
                {
                    foreach (var conflictColumn in query.ConflictColumns)
                    {
                        CollectValue(row[FindInsertColumnIndex(query.InsertColumns, conflictColumn)], parameters);
                    }

                    var updateColumns = (query.HasExplicitUpsertUpdateOnly
                            ? query.UpsertUpdateOnlyColumns
                            : query.InsertColumns)
                        .Where(column => !query.ConflictColumns.Any(key => string.Equals(key, column, StringComparison.OrdinalIgnoreCase)));
                    foreach (var updateColumn in updateColumns)
                    {
                        CollectValue(row[FindInsertColumnIndex(query.InsertColumns, updateColumn)], parameters);
                    }
                }

                return;
            }

            foreach (var insertRow in query.InsertValues)
            {
                CollectValues(insertRow, parameters);
            }

            return;
        }

        if (!string.IsNullOrWhiteSpace(query.UpdateTable))
        {
            foreach (var set in query.SetValues)
            {
                CollectValue(set.Value, parameters);
            }

            CollectWhereParameters(query.WhereTokens, parameters);
            return;
        }

        if (!string.IsNullOrWhiteSpace(query.DeleteTable))
        {
            CollectWhereParameters(query.WhereTokens, parameters);
            return;
        }

        if (query.FromSubquery.HasValue)
        {
            CollectParameters(query.FromSubquery.Value.Item1, parameters);
        }

        CollectWhereParameters(query.WhereTokens, parameters);
        foreach (var having in query.HavingExpressions)
        {
            CollectValue(having.Value, parameters);
        }

        foreach (var (_, compoundQuery) in query.CompoundQueries)
        {
            CollectParameters(compoundQuery, parameters);
        }
    }

    private void CollectWhereParameters(IReadOnlyList<IWhereToken> tokens, List<object> parameters)
    {
        foreach (var token in tokens)
        {
            switch (token)
            {
                case ConditionToken condition:
                    CollectValue(condition.Value, parameters);
                    break;
                case RawConditionToken condition:
                    CollectValue(condition.Value, parameters);
                    break;
                case InToken values:
                    CollectValues(values.Values, parameters);
                    break;
                case RawInToken values:
                    CollectValues(values.Values, parameters);
                    break;
                case NotInToken values:
                    CollectValues(values.Values, parameters);
                    break;
                case RawNotInToken values:
                    CollectValues(values.Values, parameters);
                    break;
                case BetweenToken between:
                    CollectValue(between.Start, parameters);
                    CollectValue(between.End, parameters);
                    break;
                case RawBetweenToken between:
                    CollectValue(between.Start, parameters);
                    CollectValue(between.End, parameters);
                    break;
                case NotBetweenToken between:
                    CollectValue(between.Start, parameters);
                    CollectValue(between.End, parameters);
                    break;
                case RawNotBetweenToken between:
                    CollectValue(between.Start, parameters);
                    CollectValue(between.End, parameters);
                    break;
            }
        }
    }

    private void CollectValues(IReadOnlyList<object> values, List<object> parameters)
    {
        foreach (var value in values)
        {
            CollectValue(value, parameters);
        }
    }

    private void CollectValue(object value, List<object> parameters)
    {
        switch (value)
        {
            case QueryParameterReference:
                return;
            case Query query:
                CollectParameters(query, parameters);
                return;
            default:
                parameters.Add(value);
                return;
        }
    }

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
        if (value is QueryParameterReference parameterReference)
        {
            builder.Append(GetParameterName(parameterReference.Index));
            return;
        }

        if (value is Query query)
        {
            builder.Append('(').Append(CompileInternal(query, parameters)).Append(')');
            return;
        }

        builder.Append(parameters != null ? AddParameter(value, parameters) : FormatValue(value));
    }

    private string AddParameter(object value, List<object> parameters)
    {
        var name = GetParameterName(parameters.Count);
        parameters.Add(value);
        return name;
    }

    private string GetParameterName(int index)
        => GetParameterName(_dialect, index);

    /// <summary>
    /// Gets the placeholder the compiler emits for the parameter at <paramref name="index"/>.
    /// </summary>
    internal static string GetParameterName(SqlDialect dialect, int index)
        => (dialect == SqlDialect.Oracle ? ":p" : "@p") + index.ToString(CultureInfo.InvariantCulture);

    /// <summary>Formats a value as a literal of the dialect.</summary>
    /// <remarks>
    /// Only types with a known literal form are written; any other value would have to be emitted through
    /// <see cref="object.ToString"/>, unquoted, which lets text reach the SQL as code. Such values throw
    /// <see cref="NotSupportedException"/>: compile them with parameters instead.
    /// </remarks>
    private string FormatValue(object value)
    {
        return value switch
        {
            string text => FormatStringLiteral(text),
            char character => FormatStringLiteral(character.ToString()),
            null => "NULL",
            DBNull => "NULL",
            bool boolean => FormatBooleanLiteral(boolean),
            DateTime dateTime => $"'{dateTime.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)}'",
            DateTimeOffset dateTimeOffset => $"'{dateTimeOffset.ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzz", CultureInfo.InvariantCulture)}'",
#if NET6_0_OR_GREATER
            DateOnly date => $"'{date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)}'",
            TimeOnly time => $"'{time.ToString("HH:mm:ss.fffffff", CultureInfo.InvariantCulture)}'",
#endif
            decimal number => number.ToString(CultureInfo.InvariantCulture),
            double number => FormatFloatingLiteral(number, number.ToString(CultureInfo.InvariantCulture)),
            float number => FormatFloatingLiteral(number, number.ToString(CultureInfo.InvariantCulture)),
            Enum enumeration => Convert.ToString(Convert.ChangeType(enumeration, Enum.GetUnderlyingType(enumeration.GetType()), CultureInfo.InvariantCulture), CultureInfo.InvariantCulture)!,
            sbyte or byte or short or ushort or int or uint or long or ulong => ((IFormattable)value).ToString(null, CultureInfo.InvariantCulture),
            Guid guid => $"'{guid.ToString("D", CultureInfo.InvariantCulture)}'",
            TimeSpan time => FormatTimeSpanLiteral(time),
            byte[] bytes => FormatBinaryLiteral(bytes),
            _ => throw new NotSupportedException(
                $"A value of type '{value.GetType().FullName}' has no literal SQL form. Use CompileWithParameters.")
        };
    }

    private static string FormatFloatingLiteral(double number, string text)
    {
        if (double.IsNaN(number) || double.IsInfinity(number))
        {
            throw new NotSupportedException("NaN and infinity have no literal SQL form. Use CompileWithParameters.");
        }

        return text;
    }

    /// <summary>
    /// Formats a string literal that cannot end early, whatever the server's escaping settings.
    /// </summary>
    /// <remarks>
    /// Doubling single quotes is enough where a backslash is an ordinary character. MySQL reads a backslash as an
    /// escape unless <c>NO_BACKSLASH_ESCAPES</c> is set, so <c>\'</c> would end the literal early; text with a
    /// backslash is written as a <c>_utf8mb4</c> hex literal, which reads the same in every SQL mode. PostgreSQL treats
    /// backslashes as escapes when <c>standard_conforming_strings</c> is off, so such text uses an <c>E'…'</c> literal,
    /// which always does. Literal text is not always stored exactly: SQL Server converts a non-<c>N</c> literal to the
    /// database code page and reads a backslash before a line break as a line continuation, and the MySQL hex form
    /// takes the server's default utf8mb4 collation. Use parameters when exact values matter.
    /// </remarks>
    private string FormatStringLiteral(string text)
    {
        var quoted = text.Replace("'", "''");
        if (text.IndexOf('\\') < 0)
        {
            return "'" + quoted + "'";
        }

        switch (_dialect)
        {
            case SqlDialect.MySql:
                var bytes = Encoding.UTF8.GetBytes(text);
                var hex = new StringBuilder(bytes.Length * 2 + 11).Append("_utf8mb4 0x");
                foreach (var value in bytes)
                {
                    hex.Append(value.ToString("X2", CultureInfo.InvariantCulture));
                }

                return hex.ToString();
            case SqlDialect.PostgreSql:
                return "E'" + quoted.Replace("\\", "\\\\") + "'";
            default:
                return "'" + quoted + "'";
        }
    }

    /// <summary>
    /// Formats a time literal as <c>[-][d ]hh:mm:ss[.fffffff]</c>, the form PostgreSQL intervals and MySQL <c>TIME</c> accept.
    /// </summary>
    private static string FormatTimeSpanLiteral(TimeSpan time)
    {
        var sign = time < TimeSpan.Zero ? "-" : string.Empty;
        var magnitude = time.Ticks < 0 ? (ulong)(-(time.Ticks + 1)) + 1 : (ulong)time.Ticks;
        var days = magnitude / (ulong)TimeSpan.TicksPerDay;
        var timeOfDay = new TimeSpan((long)(magnitude % (ulong)TimeSpan.TicksPerDay)).ToString("c", CultureInfo.InvariantCulture);
        return days == 0
            ? $"'{sign}{timeOfDay}'"
            : $"'{sign}{days.ToString(CultureInfo.InvariantCulture)} {timeOfDay}'";
    }

    private string FormatBinaryLiteral(byte[] bytes)
    {
        var hex = new StringBuilder(bytes.Length * 2);
        foreach (var value in bytes)
        {
            hex.Append(value.ToString("X2", CultureInfo.InvariantCulture));
        }

        return _dialect switch
        {
            SqlDialect.SqlServer => "0x" + hex,
            SqlDialect.PostgreSql => "decode('" + hex + "', 'hex')",
            SqlDialect.Oracle => "HEXTORAW('" + hex + "')",
            _ => "X'" + hex + "'"
        };
    }

    private string FormatBooleanLiteral(bool value)
        => _dialect switch
        {
            SqlDialect.PostgreSql => value ? "TRUE" : "FALSE",
            _ => value ? "1" : "0"
        };
}
