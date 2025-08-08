using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;

namespace DBAClientX.QueryBuilder;

public class QueryCompiler
{
    private readonly SqlDialect _dialect;

    public QueryCompiler(SqlDialect dialect = SqlDialect.SqlServer)
    {
        _dialect = dialect;
    }

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
        if (query.OpenGroups != 0)
        {
            throw new InvalidOperationException("Unbalanced groupings: some groups have not been closed.");
        }
        var sb = new StringBuilder();

        if (!string.IsNullOrWhiteSpace(query.InsertTable))
        {
            if (query.IsUpsert)
            {
                return CompileUpsert(query, parameters);
            }
            sb.Append("INSERT INTO ").Append(QuoteIdentifier(query.InsertTable));

            if (query.InsertColumns.Count > 0)
            {
                sb.Append(" (").Append(string.Join(", ", query.InsertColumns.Select(QuoteIdentifier))).Append(')');
            }

            if (query.InsertValues.Count > 0)
            {
                sb.Append(" VALUES ");
                bool firstRow = true;
                foreach (var row in query.InsertValues)
                {
                    if (!firstRow)
                    {
                        sb.Append(", ");
                    }
                    sb.Append('(');
                    for (int i = 0; i < row.Count; i++)
                    {
                        if (i > 0)
                        {
                            sb.Append(", ");
                        }
                        AppendValue(sb, row[i], parameters);
                    }
                    sb.Append(')');
                    firstRow = false;
                }
            }

            return sb.ToString();
        }

        if (!string.IsNullOrWhiteSpace(query.UpdateTable))
        {
            sb.Append("UPDATE ").Append(QuoteIdentifier(query.UpdateTable));
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
                    sb.Append(QuoteIdentifier(set.Column)).Append(" = ");
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
            sb.Append("DELETE FROM ").Append(QuoteIdentifier(query.DeleteTable));

            if (query.WhereTokens.Count > 0)
            {
                sb.Append(" WHERE ");
                AppendWhereTokens(sb, query.WhereTokens, parameters);
            }

            return sb.ToString();
        }

        sb.Append("SELECT ");
        if (_dialect == SqlDialect.SqlServer && query.LimitValue.HasValue && (query.UseTop || !query.OffsetValue.HasValue))
        {
            sb.Append("TOP ").Append(query.LimitValue.Value).Append(' ');
        }

        if (query.SelectColumns.Count > 0)
        {
            sb.Append(string.Join(", ", query.SelectColumns.Select(QuoteIdentifier)));
        }
        else
        {
            sb.Append("*");
        }

        if (!string.IsNullOrWhiteSpace(query.Table))
        {
            sb.Append(" FROM ").Append(QuoteIdentifier(query.Table));
        }
        else if (query.FromSubquery.HasValue)
        {
            var (subQuery, alias) = query.FromSubquery.Value;
            sb.Append(" FROM (").Append(CompileInternal(subQuery, parameters)).Append(") AS ").Append(QuoteIdentifier(alias));
        }

        if (query.Joins.Count > 0)
        {
            foreach (var join in query.Joins)
            {
                sb.Append(' ').Append(join.Type).Append(' ').Append(QuoteIdentifier(join.Table));
                if (!string.IsNullOrWhiteSpace(join.Condition))
                {
                    sb.Append(" ON ").Append(join.Condition);
                }
            }
        }

        if (query.WhereTokens.Count > 0)
        {
            sb.Append(" WHERE ");
            AppendWhereTokens(sb, query.WhereTokens, parameters);
        }

        if (query.GroupByColumns.Count > 0)
        {
            sb.Append(" GROUP BY ").Append(string.Join(", ", query.GroupByColumns.Select(QuoteIdentifier)));
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
                sb.Append(QuoteIdentifier(clause.Column)).Append(' ').Append(clause.Operator).Append(' ');
                AppendValue(sb, clause.Value, parameters);
                first = false;
            }
        }

        if (query.OrderByColumns.Count > 0)
        {
            sb.Append(" ORDER BY ").Append(string.Join(", ", query.OrderByColumns.Select(QuoteIdentifier)));
        }

        if (_dialect == SqlDialect.SqlServer)
        {
            if (query.OffsetValue.HasValue)
            {
                sb.Append(" OFFSET ").Append(query.OffsetValue.Value).Append(" ROWS");
                if (query.LimitValue.HasValue && !query.UseTop)
                {
                    sb.Append(" FETCH NEXT ").Append(query.LimitValue.Value).Append(" ROWS ONLY");
                }
            }
        }
        else
        {
            if (query.LimitValue.HasValue)
            {
                sb.Append(" LIMIT ").Append(query.LimitValue.Value);
            }
            if (query.OffsetValue.HasValue)
            {
                sb.Append(" OFFSET ").Append(query.OffsetValue.Value);
            }
        }
        if (query.CompoundQueries.Count > 0)
        {
            foreach (var (type, q) in query.CompoundQueries)
            {
                sb.Append(' ').Append(type).Append(' ').Append(CompileInternal(q, parameters));
            }
        }

        return sb.ToString();
    }

    private string CompileUpsert(Query query, List<object>? parameters)
    {
        var sb = new StringBuilder();
        var row = query.InsertValues[0];
        switch (_dialect)
        {
            case SqlDialect.PostgreSql:
            case SqlDialect.SQLite:
                sb.Append("INSERT INTO ").Append(QuoteIdentifier(query.InsertTable));
                sb.Append(" (").Append(string.Join(", ", query.InsertColumns.Select(QuoteIdentifier))).Append(") VALUES (");
                for (int i = 0; i < row.Count; i++)
                {
                    if (i > 0)
                    {
                        sb.Append(", ");
                    }
                    AppendValue(sb, row[i], parameters);
                }
                sb.Append(") ON CONFLICT (")
                  .Append(string.Join(", ", query.ConflictColumns.Select(QuoteIdentifier)))
                  .Append(") DO UPDATE SET ");
                for (int i = 0; i < query.InsertColumns.Count; i++)
                {
                    if (i > 0)
                    {
                        sb.Append(", ");
                    }
                    var col = query.InsertColumns[i];
                    sb.Append(QuoteIdentifier(col)).Append(" = EXCLUDED.").Append(QuoteIdentifier(col));
                }
                return sb.ToString();
            case SqlDialect.MySql:
                sb.Append("INSERT INTO ").Append(QuoteIdentifier(query.InsertTable));
                sb.Append(" (").Append(string.Join(", ", query.InsertColumns.Select(QuoteIdentifier))).Append(") VALUES (");
                for (int i = 0; i < row.Count; i++)
                {
                    if (i > 0)
                    {
                        sb.Append(", ");
                    }
                    AppendValue(sb, row[i], parameters);
                }
                sb.Append(") ON DUPLICATE KEY UPDATE ");
                for (int i = 0; i < query.InsertColumns.Count; i++)
                {
                    if (i > 0)
                    {
                        sb.Append(", ");
                    }
                    var col = query.InsertColumns[i];
                    sb.Append(QuoteIdentifier(col)).Append(" = VALUES(").Append(QuoteIdentifier(col)).Append(')');
                }
                return sb.ToString();
            case SqlDialect.SqlServer:
                sb.Append("MERGE INTO ").Append(QuoteIdentifier(query.InsertTable)).Append(" AS target USING (VALUES (");
                for (int i = 0; i < row.Count; i++)
                {
                    if (i > 0)
                    {
                        sb.Append(", ");
                    }
                    AppendValue(sb, row[i], parameters);
                }
                sb.Append(")) AS source (")
                  .Append(string.Join(", ", query.InsertColumns.Select(QuoteIdentifier)))
                  .Append(") ON (");
                for (int i = 0; i < query.ConflictColumns.Count; i++)
                {
                    if (i > 0)
                    {
                        sb.Append(" AND ");
                    }
                    var col = query.ConflictColumns[i];
                    sb.Append("target.").Append(QuoteIdentifier(col)).Append(" = source.").Append(QuoteIdentifier(col));
                }
                sb.Append(") WHEN MATCHED THEN UPDATE SET ");
                for (int i = 0; i < query.InsertColumns.Count; i++)
                {
                    if (i > 0)
                    {
                        sb.Append(", ");
                    }
                    var col = query.InsertColumns[i];
                    sb.Append("target.").Append(QuoteIdentifier(col)).Append(" = source.").Append(QuoteIdentifier(col));
                }
                sb.Append(" WHEN NOT MATCHED THEN INSERT (")
                  .Append(string.Join(", ", query.InsertColumns.Select(QuoteIdentifier)))
                  .Append(") VALUES (");
                for (int i = 0; i < query.InsertColumns.Count; i++)
                {
                    if (i > 0)
                    {
                        sb.Append(", ");
                    }
                    var col = query.InsertColumns[i];
                    sb.Append("source.").Append(QuoteIdentifier(col));
                }
                sb.Append(")");
                return sb.ToString();
            default:
                throw new NotSupportedException($"Upsert not supported for {_dialect}");
        }
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
                    sb.Append(QuoteIdentifier(cond.Column)).Append(' ').Append(cond.Operator).Append(' ');
                    AppendValue(sb, cond.Value, parameters);
                    break;
                case GroupStartToken:
                    sb.Append('(');
                    break;
                case GroupEndToken:
                    sb.Append(')');
                    break;
                case NullToken n:
                    sb.Append(QuoteIdentifier(n.Column)).Append(" IS NULL");
                    break;
                case NotNullToken nn:
                    sb.Append(QuoteIdentifier(nn.Column)).Append(" IS NOT NULL");
                    break;
                case InToken it:
                    sb.Append(QuoteIdentifier(it.Column)).Append(" IN (");
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
                    sb.Append(QuoteIdentifier(nit.Column)).Append(" NOT IN (");
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
                    sb.Append(QuoteIdentifier(bt.Column)).Append(" BETWEEN ");
                    AppendValue(sb, bt.Start, parameters);
                    sb.Append(" AND ");
                    AppendValue(sb, bt.End, parameters);
                    break;
                case NotBetweenToken nbt:
                    sb.Append(QuoteIdentifier(nbt.Column)).Append(" NOT BETWEEN ");
                    AppendValue(sb, nbt.Start, parameters);
                    sb.Append(" AND ");
                    AppendValue(sb, nbt.End, parameters);
                    break;
            }
        }
    }

    private string QuoteIdentifier(string identifier)
    {
        if (string.IsNullOrEmpty(identifier))
        {
            return identifier;
        }

        string suffix = string.Empty;
        if (identifier.EndsWith(" DESC", StringComparison.OrdinalIgnoreCase))
        {
            suffix = " DESC";
            identifier = identifier.Substring(0, identifier.Length - 5);
        }
        else if (identifier.EndsWith(" ASC", StringComparison.OrdinalIgnoreCase))
        {
            suffix = " ASC";
            identifier = identifier.Substring(0, identifier.Length - 4);
        }

        if (identifier == "*" || identifier.Contains(' ') || identifier.Contains('(') || identifier.Contains(')') || identifier.All(char.IsDigit))
        {
            return identifier + suffix;
        }

        var (open, close) = _dialect switch
        {
            SqlDialect.SqlServer => ('[', ']'),
            SqlDialect.MySql => ('`', '`'),
            _ => ('"', '"')
        };

        var parts = identifier.Split('.');
        var sb = new StringBuilder();
        for (int i = 0; i < parts.Length; i++)
        {
            if (i > 0)
            {
                sb.Append('.');
            }
            sb.Append(open).Append(parts[i]).Append(close);
        }

        sb.Append(suffix);
        return sb.ToString();
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
