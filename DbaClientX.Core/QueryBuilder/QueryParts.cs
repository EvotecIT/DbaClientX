using System;
using System.Collections.Generic;

namespace DBAClientX.QueryBuilder;

internal readonly record struct QueryExpression(string Text, bool IsRaw);

internal readonly record struct QueryOrderExpression(string Text, bool IsRaw, bool Descending);

internal sealed record QueryJoinClause(
    string Type,
    QueryExpression Table,
    string? Alias,
    string? LeftColumn,
    string? Operator,
    string? RightColumn,
    string? RawCondition);

internal readonly record struct QueryHavingClause(string Expression, string Operator, object Value, bool IsRaw);

internal sealed record RawConditionToken(string Expression, string Operator, object Value) : IWhereToken;

internal sealed record RawNullToken(string Expression) : IWhereToken;

internal sealed record RawNotNullToken(string Expression) : IWhereToken;

internal sealed record RawInToken(string Expression, IReadOnlyList<object> Values) : IWhereToken;

internal sealed record RawNotInToken(string Expression, IReadOnlyList<object> Values) : IWhereToken;

internal sealed record RawBetweenToken(string Expression, object Start, object End) : IWhereToken;

internal sealed record RawNotBetweenToken(string Expression, object Start, object End) : IWhereToken;

internal static class QueryComparisonOperator
{
    private static readonly HashSet<string> Allowed = new(StringComparer.Ordinal)
    {
        "=",
        "<>",
        "!=",
        "<",
        "<=",
        ">",
        ">=",
        "LIKE",
        "NOT LIKE",
        "IS",
        "IS NOT",
        "IN",
        "NOT IN",
        "<=>",
        "!<",
        "!>",
        "ILIKE",
        "NOT ILIKE",
        "SIMILAR TO",
        "NOT SIMILAR TO",
        "REGEXP",
        "RLIKE",
        "GLOB",
        "MATCH",
        "IS DISTINCT FROM",
        "IS NOT DISTINCT FROM"
    };

    public static string Normalize(string value, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("Operator cannot be null, empty, or whitespace.", parameterName);
        }

        var normalized = string.Join(" ", value.Trim().Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries))
            .ToUpperInvariant();
        if (!Allowed.Contains(normalized))
        {
            throw new ArgumentException($"Operator '{value}' is not supported by the safe query API.", parameterName);
        }

        return normalized;
    }
}
