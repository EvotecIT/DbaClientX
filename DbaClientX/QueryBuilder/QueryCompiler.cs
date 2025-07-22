using System.Text;

namespace DBAClientX.QueryBuilder;

public class QueryCompiler
{
    public string Compile(Query query)
    {
        var sb = new StringBuilder();
        sb.Append("SELECT ");
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

    private static string FormatValue(object value)
    {
        return value switch
        {
            string s => $"'{s.Replace("'", "''")}'",
            null => "NULL",
            bool b => b ? "1" : "0",
            _ => value.ToString()
        };
    }
}

