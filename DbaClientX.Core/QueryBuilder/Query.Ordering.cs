using System;

namespace DBAClientX.QueryBuilder;

public partial class Query
{
    /// <summary>Adds columns to the <c>ORDER BY</c> clause in ascending order.</summary>
    public Query OrderBy(params string[] columns)
    {
        ValidateStrings(columns, nameof(columns));
        foreach (var column in columns)
        {
            _orderBy.Add(new QueryOrderExpression(column, IsRaw: false, Descending: false));
        }
        return this;
    }

    /// <summary>Adds columns to the <c>ORDER BY</c> clause in descending order.</summary>
    public Query OrderByDescending(params string[] columns)
    {
        ValidateStrings(columns, nameof(columns));
        foreach (var column in columns)
        {
            _orderBy.Add(new QueryOrderExpression(column, IsRaw: false, Descending: true));
        }
        return this;
    }

    /// <summary>Adds caller-authored SQL expressions to the <c>ORDER BY</c> clause.</summary>
    /// <remarks>Never pass untrusted input to this method.</remarks>
    public Query OrderByRaw(params string[] expressions)
    {
        ValidateStrings(expressions, nameof(expressions));
        foreach (var expression in expressions)
        {
            _orderBy.Add(new QueryOrderExpression(expression, IsRaw: true, Descending: false));
        }
        return this;
    }

    /// <summary>Adds columns to the <c>GROUP BY</c> clause.</summary>
    public Query GroupBy(params string[] columns)
    {
        ValidateStrings(columns, nameof(columns));
        foreach (var column in columns)
        {
            _groupBy.Add(new QueryExpression(column, IsRaw: false));
        }
        return this;
    }

    /// <summary>Adds caller-authored SQL expressions to the <c>GROUP BY</c> clause.</summary>
    /// <remarks>Never pass untrusted input to this method.</remarks>
    public Query GroupByRaw(params string[] expressions)
    {
        ValidateStrings(expressions, nameof(expressions));
        foreach (var expression in expressions)
        {
            _groupBy.Add(new QueryExpression(expression, IsRaw: true));
        }
        return this;
    }

    /// <inheritdoc cref="Having(string, string, object)"/>
    public Query Having(string column, object value) => Having(column, "=", value);

    /// <summary>Adds a predicate to the <c>HAVING</c> clause.</summary>
    public Query Having(string column, string op, object value)
    {
        ValidateString(column, nameof(column));
        var normalizedOperator = QueryComparisonOperator.Normalize(op, nameof(op));
        if (value == null)
        {
            throw new ArgumentException("Value cannot be null.", nameof(value));
        }
        _having.Add(new QueryHavingClause(column, normalizedOperator, value, IsRaw: false));
        return this;
    }

    /// <summary>Adds a predicate whose left side is a caller-authored SQL expression to <c>HAVING</c>.</summary>
    /// <remarks>Never pass untrusted input to <paramref name="expression"/>.</remarks>
    public Query HavingRaw(string expression, string op, object value)
    {
        ValidateString(expression, nameof(expression));
        var normalizedOperator = QueryComparisonOperator.Normalize(op, nameof(op));
        if (value == null)
        {
            throw new ArgumentException("Value cannot be null.", nameof(value));
        }
        _having.Add(new QueryHavingClause(expression, normalizedOperator, value, IsRaw: true));
        return this;
    }

    /// <summary>Applies a non-negative <c>LIMIT</c> or provider equivalent.</summary>
    public Query Limit(int limit)
    {
        if (limit < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(limit), limit, "Limit cannot be negative.");
        }
        _limit = limit;
        _useTop = false;
        return this;
    }

    /// <summary>Applies a non-negative <c>OFFSET</c>.</summary>
    public Query Offset(int offset)
    {
        if (offset < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(offset), offset, "Offset cannot be negative.");
        }
        _offset = offset;
        _useTop = false;
        return this;
    }

    /// <summary>Applies a non-negative SQL Server-style <c>TOP</c> value.</summary>
    public Query Top(int top)
    {
        if (top < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(top), top, "Top cannot be negative.");
        }
        _limit = top;
        _useTop = true;
        _offset = null;
        return this;
    }
}
