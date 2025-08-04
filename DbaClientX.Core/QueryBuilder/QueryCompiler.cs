using System.Text;
using System.Globalization;

namespace DBAClientX.QueryBuilder;

public class QueryCompiler
{
    public string Compile(Query query)
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
                    sb.Append(FormatValue(value));
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
                    sb.Append(set.Column).Append(" = ").Append(FormatValue(set.Value));
                    firstSet = false;
                }
            }

            if (query.WhereTokens.Count > 0)
            {
                sb.Append(" WHERE ");
                AppendWhereTokens(sb, query.WhereTokens);
            }

            return sb.ToString();
        }

        if (!string.IsNullOrWhiteSpace(query.DeleteTable))
        {
            sb.Append("DELETE FROM ").Append(query.DeleteTable);

            if (query.WhereTokens.Count > 0)
            {
                sb.Append(" WHERE ");
                AppendWhereTokens(sb, query.WhereTokens);
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
            sb.Append(" FROM (").Append(Compile(subQuery)).Append(") AS ").Append(alias);
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
            AppendWhereTokens(sb, query.WhereTokens);
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
                sb.Append(FormatValue(clause.Value));
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

    private static void AppendWhereTokens(StringBuilder sb, IReadOnlyList<IWhereToken> tokens)
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
                    sb.Append(FormatValue(cond.Value));
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
            }
        }
    }

    private static string FormatValue(object value)
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
            Query q => "(" + new QueryCompiler().Compile(q) + ")",
            _ => value.ToString()
        };
    }
}

