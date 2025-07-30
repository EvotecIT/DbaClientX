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

            if (query.WhereClauses.Count > 0)
            {
                sb.Append(" WHERE ");
                bool first = true;
                foreach (var clause in query.WhereClauses)
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

            return sb.ToString();
        }

        if (!string.IsNullOrWhiteSpace(query.DeleteTable))
        {
            sb.Append("DELETE FROM ").Append(query.DeleteTable);

            if (query.WhereClauses.Count > 0)
            {
                sb.Append(" WHERE ");
                bool first = true;
                foreach (var clause in query.WhereClauses)
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

        if (query.WhereClauses.Count > 0)
        {
            sb.Append(" WHERE ");
            bool first = true;
            foreach (var clause in query.WhereClauses)
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

        if (query.OrderByColumns.Count > 0)
        {
            sb.Append(" ORDER BY ").Append(string.Join(", ", query.OrderByColumns));
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

    private static string FormatValue(object value)
    {
        return value switch
        {
            string s => $"'{s.Replace("'", "''")}'",
            null => "NULL",
            bool b => b ? "1" : "0",
            decimal d => d.ToString(CultureInfo.InvariantCulture),
            double d => d.ToString(CultureInfo.InvariantCulture),
            float f => f.ToString(CultureInfo.InvariantCulture),
            Query q => "(" + new QueryCompiler().Compile(q) + ")",
            _ => value.ToString()
        };
    }
}

