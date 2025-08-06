using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace DBAClientX.QueryBuilder;

public class QueryCompiler
{
    public string Compile(Query query)
        => CompileInternal(query, null);

    public (string Sql, IReadOnlyList<object> Parameters) CompileWithParameters(Query query)
    {
        var parameters = new List<object>();
        var sql = CompileInternal(query, parameters);
        return (sql, parameters);
    }

    private string CompileInternal(Query query, List<object>? parameters)
    {
        var sb = new StringBuilder();

        if (!string.IsNullOrWhiteSpace(query.InsertTable))
        {
            sb.Append("INSERT INTO ").Append(query.InsertTable);

            if (query.InsertColumns.Count > 0)
            {
                sb.Append(" (").Append(string.Join(", ", query.InsertColumns)).Append(')');
            }

            if (query.InsertValues.Count > 0)
            {
                sb.Append(" VALUES (");
                bool firstValue = true;
                foreach (var value in query.InsertValues)
                {
                    if (!firstValue)
                    {
                        sb.Append(", ");
                    }
                    AppendValue(sb, value, parameters);
                    firstValue = false;
                }
                sb.Append(')');
            }

            return sb.ToString();
        }

        if (!string.IsNullOrWhiteSpace(query.UpdateTable))
        {
            sb.Append("UPDATE ").Append(query.UpdateTable);
            if (query.SetValues.Count > 0)
            {
                sb.Append(" SET ");
                bool firstSet = true;
                foreach (var set in query.SetValues)
                {
                    if (!firstSet)
                    {
                        sb.Append(", ");
                    }
                    sb.Append(set.Column).Append(" = ");
                    AppendValue(sb, set.Value, parameters);
                    firstSet = false;
                }
            }

            if (query.WhereTokens.Count > 0)
            {
                sb.Append(" WHERE ");
                AppendWhereTokens(sb, query.WhereTokens, parameters);
            }

            return sb.ToString();
        }

        if (!string.IsNullOrWhiteSpace(query.DeleteTable))
        {
            sb.Append("DELETE FROM ").Append(query.DeleteTable);

            if (query.WhereTokens.Count > 0)
            {
                sb.Append(" WHERE ");
                AppendWhereTokens(sb, query.WhereTokens, parameters);
            }

            return sb.ToString();
        }

        sb.Append("SELECT ");
        if (query.LimitValue.HasValue && query.UseTop)
        {
            sb.Append("TOP ").Append(query.LimitValue.Value).Append(' ');
        }

        if (query.SelectColumns.Count > 0)
        {
            sb.Append(string.Join(", ", query.SelectColumns));
        }
        else
        {
            sb.Append("*");
        }

        if (!string.IsNullOrWhiteSpace(query.Table))
        {
            sb.Append(" FROM ").Append(query.Table);
        }
        else if (query.FromSubquery.HasValue)
        {
            var (subQuery, alias) = query.FromSubquery.Value;
            sb.Append(" FROM (").Append(CompileInternal(subQuery, parameters)).Append(") AS ").Append(alias);
        }

        if (query.Joins.Count > 0)
        {
            foreach (var join in query.Joins)
            {
                sb.Append(' ').Append(join.Type).Append(' ').Append(join.Table).Append(" ON ").Append(join.Condition);
            }
        }

        if (query.WhereTokens.Count > 0)
        {
            sb.Append(" WHERE ");
            AppendWhereTokens(sb, query.WhereTokens, parameters);
        }

        if (query.GroupByColumns.Count > 0)
        {
            sb.Append(" GROUP BY ").Append(string.Join(", ", query.GroupByColumns));
        }

        if (query.HavingClauses.Count > 0)
        {
            sb.Append(" HAVING ");
            bool first = true;
            foreach (var clause in query.HavingClauses)
            {
                if (!first)
                {
                    sb.Append(" AND ");
                }
                sb.Append(clause.Column).Append(' ').Append(clause.Operator).Append(' ');
                AppendValue(sb, clause.Value, parameters);
                first = false;
            }
        }

        if (query.OrderByExpressions.Count > 0)
        {
            sb.Append(" ORDER BY ").Append(string.Join(", ", query.OrderByExpressions));
        }

        if (query.LimitValue.HasValue && !query.UseTop)
        {
            sb.Append(" LIMIT ").Append(query.LimitValue.Value);

            if (query.OffsetValue.HasValue)
            {
                sb.Append(" OFFSET ").Append(query.OffsetValue.Value);
            }
        }
        else if (query.OffsetValue.HasValue)
        {
            sb.Append(" OFFSET ").Append(query.OffsetValue.Value);
        }

        return sb.ToString();
    }

    private void AppendWhereTokens(StringBuilder sb, IReadOnlyList<IWhereToken> tokens, List<object>? parameters)
    {
        foreach (var token in tokens)
        {
            switch (token)
            {
                case OperatorToken op:
                    sb.Append(' ').Append(op.Operator).Append(' ');
                    break;
                case ConditionToken cond:
                    sb.Append(cond.Column).Append(' ').Append(cond.Operator).Append(' ');
                    AppendValue(sb, cond.Value, parameters);
                    break;
                case GroupStartToken:
                    sb.Append('(');
                    break;
                case GroupEndToken:
                    sb.Append(')');
                    break;
                case NullToken n:
                    sb.Append(n.Column).Append(" IS NULL");
                    break;
                case NotNullToken nn:
                    sb.Append(nn.Column).Append(" IS NOT NULL");
                    break;
                case InToken it:
                    sb.Append(it.Column).Append(" IN (");
                    for (int i = 0; i < it.Values.Count; i++)
                    {
                        if (i > 0)
                        {
                            sb.Append(", ");
                        }
                        AppendValue(sb, it.Values[i], parameters);
                    }
                    sb.Append(')');
                    break;
                case NotInToken nit:
                    sb.Append(nit.Column).Append(" NOT IN (");
                    for (int i = 0; i < nit.Values.Count; i++)
                    {
                        if (i > 0)
                        {
                            sb.Append(", ");
                        }
                        AppendValue(sb, nit.Values[i], parameters);
                    }
                    sb.Append(')');
                    break;
                case BetweenToken bt:
                    sb.Append(bt.Column).Append(" BETWEEN ");
                    AppendValue(sb, bt.Start, parameters);
                    sb.Append(" AND ");
                    AppendValue(sb, bt.End, parameters);
                    break;
                case NotBetweenToken nbt:
                    sb.Append(nbt.Column).Append(" NOT BETWEEN ");
                    AppendValue(sb, nbt.Start, parameters);
                    sb.Append(" AND ");
                    AppendValue(sb, nbt.End, parameters);
                    break;
            }
        }
    }

    private void AppendValue(StringBuilder sb, object value, List<object>? parameters)
    {
        if (value is Query q)
        {
            sb.Append('(').Append(CompileInternal(q, parameters)).Append(')');
            return;
        }

        if (parameters != null)
        {
            sb.Append(AddParameter(value, parameters));
        }
        else
        {
            sb.Append(FormatValue(value));
        }
    }

    private string AddParameter(object value, List<object> parameters)
    {
        var name = "@p" + parameters.Count;
        parameters.Add(value);
        return name;
    }

    private string FormatValue(object value)
    {
        return value switch
        {
            string s => $"'{s.Replace("'", "''")}'",
            null => "NULL",
            bool b => b ? "1" : "0",
            DateTime dt => $"'{dt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)}'",
            DateTimeOffset dto => $"'{dto.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)}'",
            decimal d => d.ToString(CultureInfo.InvariantCulture),
            double d => d.ToString(CultureInfo.InvariantCulture),
            float f => f.ToString(CultureInfo.InvariantCulture),
            _ => value.ToString()
        };
    }
}

