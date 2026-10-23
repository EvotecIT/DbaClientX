using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace DBAClientX.QueryBuilder;

public partial class QueryCompiler
{
    private void AppendValueRank(StringBuilder sql, SqlValueRank rank, List<object>? parameters)
    {
        if (_dialect != SqlDialect.SQLite)
            throw new NotSupportedException("Ordinal category ranks are currently qualified for SQLite only.");
        if (parameters == null)
            throw new InvalidOperationException("Category ranks contain bound values. Use CompileWithParameters or CompileWithNamedParameters.");
        sql.Append("CASE (").Append(QuoteIdentifier(rank.Column)).Append(" COLLATE BINARY)");
        for (var index = 0; index < rank.Tokens.Count; index++)
        {
            sql.Append(" WHEN ");
            AppendValue(sql, rank.Tokens[index], parameters);
            sql.Append(" THEN ").Append(index.ToString(CultureInfo.InvariantCulture));
        }
        sql.Append(" ELSE ").Append(rank.Tokens.Count.ToString(CultureInfo.InvariantCulture)).Append(" END");
    }

    private static void CollectValueRank(SqlValueRank rank, List<object> parameters)
    {
        foreach (var token in rank.Tokens) parameters.Add(token);
    }

    private static void AppendValueRankCacheKey(StringBuilder key, SqlValueRank rank)
    {
        key.Append("VR:");
        AppendCacheText(key, rank.Column);
        key.Append(':').Append(rank.Tokens.Count).Append('|');
    }
}
