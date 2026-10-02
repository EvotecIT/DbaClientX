using System.Collections.Generic;
using System.Text;

namespace DBAClientX.QueryBuilder;

public partial class QueryCompiler
{
    private void AppendFunctionCondition(StringBuilder sql, FunctionConditionToken function, List<object>? parameters)
    {
        sql.Append(function.Function).Append('(').Append(function.Expression);
        foreach (var argument in function.Arguments) { sql.Append(", "); AppendValue(sql, argument, parameters); }
        sql.Append(") ").Append(function.Operator).Append(' ');
        AppendValue(sql, function.Value, parameters);
    }

    private void AppendFunctionCacheKey(StringBuilder key, FunctionConditionToken function)
    {
        key.Append("WCF:");
        AppendCacheText(key, function.Function);
        AppendCacheText(key, function.Expression);
        AppendCacheText(key, function.Operator);
        key.Append(function.Arguments.Count).Append(':');
        foreach (var argument in function.Arguments) AppendCacheValueShape(key, argument);
        AppendCacheValueShape(key, function.Value);
        key.Append('|');
    }
}
