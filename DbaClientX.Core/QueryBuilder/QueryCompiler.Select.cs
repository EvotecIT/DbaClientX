using System;
using System.Collections.Generic;
using System.Text;

namespace DBAClientX.QueryBuilder;

public partial class QueryCompiler
{
    private string CompileSelect(Query query, List<object>? parameters, StringBuilder sb)
    {
        bool compound = query.CompoundQueries.Count > 0;
        sb.Append("SELECT ");
        if (query.IsDistinct)
        {
            sb.Append("DISTINCT ");
        }
        if (!compound && UsesTop(query))
        {
            sb.Append("TOP ").Append(query.LimitValue.GetValueOrDefault()).Append(' ');
        }

        if (query.SelectExpressions.Count > 0)
        {
            var allowStandaloneLiterals = query.TableExpression == null && !query.FromSubquery.HasValue;
            for (var index = 0; index < query.SelectExpressions.Count; index++)
            {
                if (index > 0)
                {
                    sb.Append(", ");
                }

                var expression = query.SelectExpressions[index];
                sb.Append(expression.IsRaw
                    ? expression.Text
                    : QuoteSelectColumn(expression.Text, allowStandaloneLiterals));
            }
        }
        else
        {
            sb.Append("*");
        }

        if (query.TableExpression is { } fromExpression)
        {
            sb.Append(" FROM ").Append(fromExpression.IsRaw ? fromExpression.Text : QuoteIdentifier(fromExpression.Text));
            if (!string.IsNullOrWhiteSpace(query.TableAlias))
            {
                AppendAlias(sb, query.TableAlias!);
            }
        }
        else if (query.FromSubquery.HasValue)
        {
            var (subQuery, alias) = query.FromSubquery.Value;
            sb.Append(" FROM (").Append(CompileInternal(subQuery, parameters)).Append(')');
            AppendAlias(sb, alias);
        }

        if (query.JoinClauses.Count > 0)
        {
            foreach (var join in query.JoinClauses)
            {
                sb.Append(' ').Append(join.Type).Append(' ')
                    .Append(join.Table.IsRaw ? join.Table.Text : QuoteIdentifier(join.Table.Text));
                if (!string.IsNullOrWhiteSpace(join.Alias))
                {
                    AppendAlias(sb, join.Alias!);
                }
                if (!string.IsNullOrWhiteSpace(join.RawCondition))
                {
                    sb.Append(" ON ").Append(join.RawCondition);
                }
                else if (!string.IsNullOrWhiteSpace(join.LeftColumn))
                {
                    sb.Append(" ON ").Append(QuoteIdentifier(join.LeftColumn!))
                        .Append(' ').Append(join.Operator).Append(' ')
                        .Append(QuoteIdentifier(join.RightColumn!));
                }
            }
        }

        if (query.WhereTokens.Count > 0)
        {
            sb.Append(" WHERE ");
            AppendWhereTokens(sb, query.WhereTokens, parameters);
        }

        if (query.GroupByExpressions.Count > 0)
        {
            sb.Append(" GROUP BY ");
            for (var index = 0; index < query.GroupByExpressions.Count; index++)
            {
                if (index > 0)
                {
                    sb.Append(", ");
                }
                var expression = query.GroupByExpressions[index];
                sb.Append(expression.IsRaw ? expression.Text : QuoteIdentifier(expression.Text));
            }
        }

        if (query.HavingExpressions.Count > 0)
        {
            sb.Append(" HAVING ");
            bool first = true;
            foreach (var clause in query.HavingExpressions)
            {
                if (!first)
                {
                    sb.Append(" AND ");
                }
                sb.Append(clause.IsRaw ? clause.Expression : QuoteIdentifier(clause.Expression))
                    .Append(' ').Append(clause.Operator).Append(' ');
                AppendValue(sb, clause.Value, parameters);
                first = false;
            }
        }

        AppendCompoundQueries(sb, query, parameters);

        if (compound && UsesTop(query))
        {
            string body = sb.ToString();
            sb.Clear();
            AppendDerivedSelect(sb, body, "dbx_compound", query, query.LimitValue.GetValueOrDefault());
        }

        if (query.OrderByExpressions.Count > 0)
        {
            sb.Append(" ORDER BY ");
            for (var index = 0; index < query.OrderByExpressions.Count; index++)
            {
                if (index > 0)
                {
                    sb.Append(", ");
                }
                var expression = query.OrderByExpressions[index];
                sb.Append(expression.IsRaw ? expression.Text : QuoteIdentifier(expression.Text));
                if (expression.Collation != null)
                {
                    AppendCollation(sb, expression.Collation);
                }

                if (expression.Descending)
                {
                    sb.Append(" DESC");
                }
            }
        }

        if (_dialect == SqlDialect.SqlServer)
        {
            if (query.OffsetValue.HasValue && query.LimitValue != 0)
            {
                if (query.OrderByExpressions.Count == 0)
                {
                    throw new InvalidOperationException("SQL Server OFFSET/FETCH requires ORDER BY.");
                }
                sb.Append(" OFFSET ").Append(query.OffsetValue.Value).Append(" ROWS");
                if (query.LimitValue.HasValue && !query.UseTop)
                {
                    sb.Append(" FETCH NEXT ").Append(query.LimitValue.Value).Append(" ROWS ONLY");
                }
            }
        }
        else if (_dialect == SqlDialect.Oracle)
        {
            if (query.OffsetValue.HasValue)
            {
                sb.Append(" OFFSET ").Append(query.OffsetValue.Value).Append(" ROWS");
                if (query.LimitValue.HasValue && !query.UseTop)
                {
                    sb.Append(" FETCH NEXT ").Append(query.LimitValue.Value).Append(" ROWS ONLY");
                }
            }
            else if (query.LimitValue.HasValue)
            {
                sb.Append(" FETCH FIRST ").Append(query.LimitValue.Value).Append(" ROWS ONLY");
            }
        }
        else
        {
            if (query.LimitValue.HasValue)
            {
                sb.Append(" LIMIT ").Append(query.LimitValue.Value);
            }
            else if (query.OffsetValue.HasValue && _dialect is SqlDialect.SQLite or SqlDialect.MySql)
            {
                sb.Append(_dialect == SqlDialect.SQLite ? " LIMIT -1" : " LIMIT 18446744073709551615");
            }
            if (query.OffsetValue.HasValue)
            {
                sb.Append(" OFFSET ").Append(query.OffsetValue.Value);
            }
        }
        return sb.ToString();
    }

    private bool UsesTop(Query query)
        => _dialect == SqlDialect.SqlServer && query.LimitValue.HasValue &&
           (query.UseTop || !query.OffsetValue.HasValue || query.LimitValue == 0);

    private static bool IsSelectQuery(Query query)
        => string.IsNullOrWhiteSpace(query.InsertTable) && string.IsNullOrWhiteSpace(query.UpdateTable) &&
           string.IsNullOrWhiteSpace(query.DeleteTable);

    private void AppendCompoundQueries(StringBuilder sb, Query query, List<object>? parameters)
    {
        string? previousOperator = null;
        for (int index = 0; index < query.CompoundQueries.Count; index++)
        {
            var (type, operand) = query.CompoundQueries[index];
            if (!IsSelectQuery(operand))
                throw new InvalidOperationException("Compound queries require SELECT operands.");

            // Preserve the builder's left-to-right composition across dialects with different set precedence.
            if (previousOperator != null && type != previousOperator &&
                !(previousOperator.StartsWith("UNION", StringComparison.Ordinal) && type.StartsWith("UNION", StringComparison.Ordinal)))
            {
                string left = sb.ToString();
                sb.Clear();
                AppendDerivedSelect(sb, left, "dbx_left_" + index, query);
            }
            sb.Append(' ').Append(type).Append(' ');

            bool scoped = operand.CompoundQueries.Count > 0 || operand.OrderByExpressions.Count > 0 ||
                          operand.LimitValue.HasValue || operand.OffsetValue.HasValue;
            if (scoped)
            {
                if (_dialect == SqlDialect.SqlServer && operand.OrderByExpressions.Count > 0 &&
                    !operand.LimitValue.HasValue && !operand.OffsetValue.HasValue)
                    throw new InvalidOperationException("SQL Server ORDER BY in a compound operand requires Limit or Offset. Order the combined query instead.");
                AppendDerivedSelect(sb, CompileInternal(operand, parameters), "dbx_operand_" + index, operand);
            }
            else
                sb.Append(CompileInternal(operand, parameters));
            previousOperator = type;
        }
    }

}
