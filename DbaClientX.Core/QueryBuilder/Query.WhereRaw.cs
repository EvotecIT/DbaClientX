namespace DBAClientX.QueryBuilder;

public partial class Query
{
    /// <summary>Adds a caller-authored SQL expression compared for equality.</summary>
    /// <remarks>Never pass untrusted input to <paramref name="expression"/>.</remarks>
    public Query WhereRaw(string expression, object value)
        => AddCondition(expression, "=", value, isRawExpression: true);

    /// <summary>Adds a caller-authored SQL expression to an <c>OR</c> equality predicate.</summary>
    /// <remarks>Never pass untrusted input to <paramref name="expression"/>.</remarks>
    public Query OrWhereRaw(string expression, object value)
        => AddCondition(expression, "=", value, "OR", isRawExpression: true);

    /// <summary>Adds an <c>IN</c> predicate for a caller-authored SQL expression.</summary>
    /// <remarks>Never pass untrusted input to <paramref name="expression"/>.</remarks>
    public Query WhereInRaw(string expression, params object[] values)
        => AddInCondition(expression, values, isRawExpression: true);

    /// <summary>Adds an <c>IN</c> predicate comparing a caller-authored SQL expression to a subquery.</summary>
    /// <remarks>Never pass untrusted input to <paramref name="expression"/>.</remarks>
    public Query WhereInRaw(string expression, Query subQuery)
        => AddCondition(expression, "IN", subQuery, isRawExpression: true);

    /// <summary>Adds an <c>OR IN</c> predicate for a caller-authored SQL expression.</summary>
    /// <remarks>Never pass untrusted input to <paramref name="expression"/>.</remarks>
    public Query OrWhereInRaw(string expression, params object[] values)
        => AddInCondition(expression, values, "OR", isRawExpression: true);

    /// <summary>Adds an <c>OR IN</c> predicate comparing a caller-authored SQL expression to a subquery.</summary>
    /// <remarks>Never pass untrusted input to <paramref name="expression"/>.</remarks>
    public Query OrWhereInRaw(string expression, Query subQuery)
        => AddCondition(expression, "IN", subQuery, "OR", isRawExpression: true);

    /// <summary>Adds a <c>NOT IN</c> predicate for a caller-authored SQL expression.</summary>
    /// <remarks>Never pass untrusted input to <paramref name="expression"/>.</remarks>
    public Query WhereNotInRaw(string expression, params object[] values)
        => AddInCondition(expression, values, not: true, isRawExpression: true);

    /// <summary>Adds a <c>NOT IN</c> predicate comparing a caller-authored SQL expression to a subquery.</summary>
    /// <remarks>Never pass untrusted input to <paramref name="expression"/>.</remarks>
    public Query WhereNotInRaw(string expression, Query subQuery)
        => AddCondition(expression, "NOT IN", subQuery, isRawExpression: true);

    /// <summary>Adds an <c>OR NOT IN</c> predicate for a caller-authored SQL expression.</summary>
    /// <remarks>Never pass untrusted input to <paramref name="expression"/>.</remarks>
    public Query OrWhereNotInRaw(string expression, params object[] values)
        => AddInCondition(expression, values, "OR", not: true, isRawExpression: true);

    /// <summary>Adds an <c>OR NOT IN</c> predicate comparing a caller-authored SQL expression to a subquery.</summary>
    /// <remarks>Never pass untrusted input to <paramref name="expression"/>.</remarks>
    public Query OrWhereNotInRaw(string expression, Query subQuery)
        => AddCondition(expression, "NOT IN", subQuery, "OR", isRawExpression: true);

    /// <summary>Adds a <c>BETWEEN</c> predicate for a caller-authored SQL expression.</summary>
    /// <remarks>Never pass untrusted input to <paramref name="expression"/>.</remarks>
    public Query WhereBetweenRaw(string expression, object start, object end)
        => AddBetweenCondition(expression, start, end, isRawExpression: true);

    /// <summary>Adds an <c>OR BETWEEN</c> predicate for a caller-authored SQL expression.</summary>
    /// <remarks>Never pass untrusted input to <paramref name="expression"/>.</remarks>
    public Query OrWhereBetweenRaw(string expression, object start, object end)
        => AddBetweenCondition(expression, start, end, "OR", isRawExpression: true);

    /// <summary>Adds a <c>NOT BETWEEN</c> predicate for a caller-authored SQL expression.</summary>
    /// <remarks>Never pass untrusted input to <paramref name="expression"/>.</remarks>
    public Query WhereNotBetweenRaw(string expression, object start, object end)
        => AddBetweenCondition(expression, start, end, not: true, isRawExpression: true);

    /// <summary>Adds an <c>OR NOT BETWEEN</c> predicate for a caller-authored SQL expression.</summary>
    /// <remarks>Never pass untrusted input to <paramref name="expression"/>.</remarks>
    public Query OrWhereNotBetweenRaw(string expression, object start, object end)
        => AddBetweenCondition(expression, start, end, "OR", not: true, isRawExpression: true);

    /// <summary>Adds an <c>IS NULL</c> predicate for a caller-authored SQL expression.</summary>
    /// <remarks>Never pass untrusted input to <paramref name="expression"/>.</remarks>
    public Query WhereNullRaw(string expression)
        => AddNullCondition(expression, isRawExpression: true);

    /// <summary>Adds an <c>OR IS NULL</c> predicate for a caller-authored SQL expression.</summary>
    /// <remarks>Never pass untrusted input to <paramref name="expression"/>.</remarks>
    public Query OrWhereNullRaw(string expression)
        => AddNullCondition(expression, "OR", isRawExpression: true);

    /// <summary>Adds an <c>IS NOT NULL</c> predicate for a caller-authored SQL expression.</summary>
    /// <remarks>Never pass untrusted input to <paramref name="expression"/>.</remarks>
    public Query WhereNotNullRaw(string expression)
        => AddNotNullCondition(expression, isRawExpression: true);

    /// <summary>Adds an <c>OR IS NOT NULL</c> predicate for a caller-authored SQL expression.</summary>
    /// <remarks>Never pass untrusted input to <paramref name="expression"/>.</remarks>
    public Query OrWhereNotNullRaw(string expression)
        => AddNotNullCondition(expression, "OR", isRawExpression: true);
}
