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
        => (_dialect == SqlDialect.Oracle ? ":p" : "@p") + index;

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
