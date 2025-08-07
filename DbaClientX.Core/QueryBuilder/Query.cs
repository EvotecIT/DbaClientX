using System;
using System.Collections.Generic;

namespace DBAClientX.QueryBuilder;

/// <summary>
/// Provides a fluent API for building SQL queries.
/// </summary>
public class Query
{
    private readonly List<string> _select = new();
    private string _from;
    private (Query Query, string Alias)? _fromSubquery;
    private readonly List<(string Type, string Table, string? Condition)> _joins = new();
    private readonly List<IWhereToken> _where = new();
    private string _insertTable;
    private readonly List<string> _insertColumns = new();
    private readonly List<object> _values = new();
    private string _updateTable;
    private readonly List<(string Column, object Value)> _set = new();
    private string _deleteTable;
    private readonly List<string> _orderBy = new();
    private readonly List<string> _groupBy = new();
    private readonly List<(string Column, string Operator, object Value)> _having = new();
    private int? _limit;
    private int? _offset;
    private bool _useTop;
    private readonly List<(string Type, Query Query)> _compoundQueries = new();

    /// <summary>
    /// Adds columns to the SELECT clause of the query.
    /// </summary>
    /// <param name="columns">The column names to select.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query Select(params string[] columns)
    {
        ValidateStrings(columns, nameof(columns));
        _select.AddRange(columns);
        return this;
    }

    /// <summary>
    /// Specifies the table to select from.
    /// </summary>
    /// <param name="table">The table name.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query From(string table)
    {
        ValidateString(table, nameof(table));
        _from = table;
        _fromSubquery = null;
        return this;
    }

    /// <summary>
    /// Specifies a subquery and alias to select from.
    /// </summary>
    /// <param name="subQuery">The subquery to use as the data source.</param>
    /// <param name="alias">The alias for the subquery.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query From(Query subQuery, string alias)
    {
        if (subQuery == null)
        {
            throw new ArgumentException("Subquery cannot be null.", nameof(subQuery));
        }
        ValidateString(alias, nameof(alias));
        _from = null;
        _fromSubquery = (subQuery, alias);
        return this;
    }

    /// <summary>
    /// Adds an INNER JOIN clause to the query.
    /// </summary>
    /// <param name="table">The table to join.</param>
    /// <param name="condition">The join condition.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query Join(string table, string condition)
    {
        ValidateString(table, nameof(table));
        ValidateString(condition, nameof(condition));
        _joins.Add(("JOIN", table, condition));
        return this;
    }

    /// <summary>
    /// Adds a LEFT JOIN clause to the query.
    /// </summary>
    /// <param name="table">The table to join.</param>
    /// <param name="condition">The join condition.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query LeftJoin(string table, string condition)
    {
        ValidateString(table, nameof(table));
        ValidateString(condition, nameof(condition));
        _joins.Add(("LEFT JOIN", table, condition));
        return this;
    }

    /// <summary>
    /// Adds a RIGHT JOIN clause to the query.
    /// </summary>
    /// <param name="table">The table to join.</param>
    /// <param name="condition">The join condition.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query RightJoin(string table, string condition)
    {
        ValidateString(table, nameof(table));
        ValidateString(condition, nameof(condition));
        _joins.Add(("RIGHT JOIN", table, condition));
        return this;
    }

    /// <summary>
    /// Adds a CROSS JOIN clause to the query.
    /// </summary>
    /// <param name="table">The table to join.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query CrossJoin(string table)
    {
        ValidateString(table, nameof(table));
        _joins.Add(("CROSS JOIN", table, null));
        return this;
    }

    /// <summary>
    /// Adds a FULL OUTER JOIN clause to the query.
    /// </summary>
    /// <param name="table">The table to join.</param>
    /// <param name="condition">The join condition.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query FullOuterJoin(string table, string condition)
    {
        ValidateString(table, nameof(table));
        ValidateString(condition, nameof(condition));
        _joins.Add(("FULL OUTER JOIN", table, condition));
        return this;
    }

    /// <summary>
    /// Adds a WHERE clause with equality comparison.
    /// </summary>
    /// <param name="column">The column to filter.</param>
    /// <param name="value">The value to compare against.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query Where(string column, object value)
    {
        return Where(column, "=", value);
    }

    /// <summary>
    /// Adds a WHERE clause with the specified operator and value.
    /// </summary>
    /// <param name="column">The column to filter.</param>
    /// <param name="op">The comparison operator.</param>
    /// <param name="value">The value to compare against.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query Where(string column, string op, object value)
    {
        return AddCondition(column, op, value);
    }

    /// <summary>
    /// Adds a WHERE clause comparing a column to a subquery.
    /// </summary>
    /// <param name="column">The column to filter.</param>
    /// <param name="op">The comparison operator.</param>
    /// <param name="subQuery">The subquery to compare with.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query Where(string column, string op, Query subQuery)
    {
        return AddCondition(column, op, subQuery);
    }

    /// <summary>
    /// Adds an OR WHERE clause with equality comparison.
    /// </summary>
    /// <param name="column">The column to filter.</param>
    /// <param name="value">The value to compare against.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query OrWhere(string column, object value)
    {
        return OrWhere(column, "=", value);
    }

    /// <summary>
    /// Adds an OR WHERE clause with the specified operator and value.
    /// </summary>
    /// <param name="column">The column to filter.</param>
    /// <param name="op">The comparison operator.</param>
    /// <param name="value">The value to compare against.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query OrWhere(string column, string op, object value)
    {
        return AddCondition(column, op, value, "OR");
    }

    /// <summary>
    /// Adds an OR WHERE clause comparing a column to a subquery.
    /// </summary>
    /// <param name="column">The column to filter.</param>
    /// <param name="op">The comparison operator.</param>
    /// <param name="subQuery">The subquery to compare with.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query OrWhere(string column, string op, Query subQuery)
    {
        return AddCondition(column, op, subQuery, "OR");
    }

    public Query WhereIn(string column, params object[] values)
    {
        return AddInCondition(column, values);
    }

    public Query WhereIn(string column, Query subQuery)
    {
        return AddCondition(column, "IN", subQuery);
    }

    public Query OrWhereIn(string column, params object[] values)
    {
        return AddInCondition(column, values, "OR");
    }

    public Query OrWhereIn(string column, Query subQuery)
    {
        return AddCondition(column, "IN", subQuery, "OR");
    }

    public Query WhereNotIn(string column, params object[] values)
    {
        return AddInCondition(column, values, null, true);
    }

    public Query WhereNotIn(string column, Query subQuery)
    {
        return AddCondition(column, "NOT IN", subQuery);
    }

    public Query OrWhereNotIn(string column, params object[] values)
    {
        return AddInCondition(column, values, "OR", true);
    }

    public Query OrWhereNotIn(string column, Query subQuery)
    {
        return AddCondition(column, "NOT IN", subQuery, "OR");
    }

    public Query WhereBetween(string column, object start, object end)
    {
        return AddBetweenCondition(column, start, end);
    }

    public Query OrWhereBetween(string column, object start, object end)
    {
        return AddBetweenCondition(column, start, end, "OR");
    }

    /// <summary>
    /// Adds a NOT BETWEEN condition to the query.
    /// </summary>
    /// <param name="column">The column to filter.</param>
    /// <param name="start">The lower bound.</param>
    /// <param name="end">The upper bound.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query WhereNotBetween(string column, object start, object end)
    {
        return AddBetweenCondition(column, start, end, null, true);
    }

    /// <summary>
    /// Adds an OR NOT BETWEEN condition to the query.
    /// </summary>
    /// <param name="column">The column to filter.</param>
    /// <param name="start">The lower bound.</param>
    /// <param name="end">The upper bound.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query OrWhereNotBetween(string column, object start, object end)
    {
        return AddBetweenCondition(column, start, end, "OR", true);
    }

    /// <summary>
    /// Adds a WHERE clause checking if a column is NULL.
    /// </summary>
    /// <param name="column">The column to check.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query WhereNull(string column)
    {
        return AddNullCondition(column);
    }

    /// <summary>
    /// Adds an OR WHERE clause checking if a column is NULL.
    /// </summary>
    /// <param name="column">The column to check.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query OrWhereNull(string column)
    {
        return AddNullCondition(column, "OR");
    }

    /// <summary>
    /// Adds a WHERE clause checking if a column is NOT NULL.
    /// </summary>
    /// <param name="column">The column to check.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query WhereNotNull(string column)
    {
        return AddNotNullCondition(column);
    }

    /// <summary>
    /// Adds an OR WHERE clause checking if a column is NOT NULL.
    /// </summary>
    /// <param name="column">The column to check.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query OrWhereNotNull(string column)
    {
        return AddNotNullCondition(column, "OR");
    }

    /// <summary>
    /// Begins a grouped set of WHERE clauses.
    /// </summary>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query BeginGroup()
    {
        AddDefaultAndIfRequired();
        _where.Add(new GroupStartToken());
        return this;
    }

    /// <summary>
    /// Ends a grouped set of WHERE clauses.
    /// </summary>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query EndGroup()
    {
        _where.Add(new GroupEndToken());
        return this;
    }

    /// <summary>
    /// Adds a logical OR to the WHERE clause.
    /// </summary>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query Or()
    {
        _where.Add(new OperatorToken("OR"));
        return this;
    }

    private Query AddCondition(string column, string op, object value, string? logical = null)
    {
        ValidateString(column, nameof(column));
        ValidateString(op, nameof(op));
        if (value == null)
        {
            throw new ArgumentException("Value cannot be null.", nameof(value));
        }
        AddLogicalOperator(logical);
        _where.Add(new ConditionToken(column, op, value));
        return this;
    }

    private Query AddNullCondition(string column, string? logical = null)
    {
        ValidateString(column, nameof(column));
        AddLogicalOperator(logical);
        _where.Add(new NullToken(column));
        return this;
    }

    private Query AddNotNullCondition(string column, string? logical = null)
    {
        ValidateString(column, nameof(column));
        AddLogicalOperator(logical);
        _where.Add(new NotNullToken(column));
        return this;
    }

    private Query AddInCondition(string column, object[] values, string? logical = null, bool not = false)
    {
        ValidateString(column, nameof(column));
        if (values == null || values.Length == 0)
        {
            throw new ArgumentException("Values cannot be null or empty.", nameof(values));
        }
        foreach (var value in values)
        {
            if (value == null)
            {
                throw new ArgumentException("Values cannot contain null.", nameof(values));
            }
        }
        AddLogicalOperator(logical);
        var list = new List<object>(values);
        if (not)
        {
            _where.Add(new NotInToken(column, list));
        }
        else
        {
            _where.Add(new InToken(column, list));
        }
        return this;
    }

    private Query AddBetweenCondition(string column, object start, object end, string? logical = null, bool not = false)
    {
        ValidateString(column, nameof(column));
        if (start == null || end == null)
        {
            throw new ArgumentException("Between values cannot be null.");
        }
        AddLogicalOperator(logical);
        if (not)
        {
            _where.Add(new NotBetweenToken(column, start, end));
        }
        else
        {
            _where.Add(new BetweenToken(column, start, end));
        }
        return this;
    }

    private void AddLogicalOperator(string? logical)
    {
        if (_where.Count > 0)
        {
            if (logical != null)
            {
                _where.Add(new OperatorToken(logical));
            }
            else if (_where[_where.Count - 1] is not OperatorToken && _where[_where.Count - 1] is not GroupStartToken)
            {
                _where.Add(new OperatorToken("AND"));
            }
        }
    }

    private void AddDefaultAndIfRequired()
    {
        if (_where.Count > 0 && _where[_where.Count - 1] is not OperatorToken && _where[_where.Count - 1] is not GroupStartToken)
        {
            _where.Add(new OperatorToken("AND"));
        }
    }

    /// <summary>
    /// Specifies the table and columns for an INSERT statement.
    /// </summary>
    /// <param name="table">The table to insert into.</param>
    /// <param name="columns">The columns to populate.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query InsertInto(string table, params string[] columns)
    {
        ValidateString(table, nameof(table));
        ValidateStrings(columns, nameof(columns));
        _insertTable = table;
        _insertColumns.AddRange(columns);
        return this;
    }

    /// <summary>
    /// Specifies the table for an UPDATE statement.
    /// </summary>
    /// <param name="table">The table to update.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query Update(string table)
    {
        ValidateString(table, nameof(table));
        _updateTable = table;
        return this;
    }

    /// <summary>
    /// Adds a column value assignment for an UPDATE statement.
    /// </summary>
    /// <param name="column">The column to update.</param>
    /// <param name="value">The value to assign.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query Set(string column, object value)
    {
        ValidateString(column, nameof(column));
        if (value == null)
        {
            throw new ArgumentException("Value cannot be null.", nameof(value));
        }
        _set.Add((column, value));
        return this;
    }

    /// <summary>
    /// Specifies the table for a DELETE statement.
    /// </summary>
    /// <param name="table">The table to delete from.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query DeleteFrom(string table)
    {
        ValidateString(table, nameof(table));
        _deleteTable = table;
        return this;
    }

    /// <summary>
    /// Adds columns to the ORDER BY clause.
    /// </summary>
    /// <param name="columns">The columns to sort by.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query OrderBy(params string[] columns)
    {
        ValidateStrings(columns, nameof(columns));
        _orderBy.AddRange(columns);
        return this;
    }

    /// <summary>
    /// Adds columns to the ORDER BY clause in descending order.
    /// </summary>
    /// <param name="columns">The columns to sort by.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query OrderByDescending(params string[] columns)
    {
        ValidateStrings(columns, nameof(columns));
        foreach (var column in columns)
        {
            _orderBy.Add($"{column} DESC");
        }
        return this;
    }

    /// <summary>
    /// Adds raw expressions to the ORDER BY clause.
    /// </summary>
    /// <param name="expressions">The expressions to order by.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query OrderByRaw(params string[] expressions)
    {
        ValidateStrings(expressions, nameof(expressions));
        _orderBy.AddRange(expressions);
        return this;
    }

    /// <summary>
    /// Adds columns to the GROUP BY clause.
    /// </summary>
    /// <param name="columns">The columns to group by.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query GroupBy(params string[] columns)
    {
        ValidateStrings(columns, nameof(columns));
        _groupBy.AddRange(columns);
        return this;
    }

    /// <summary>
    /// Adds a HAVING clause with equality comparison.
    /// </summary>
    /// <param name="column">The column to filter.</param>
    /// <param name="value">The value to compare against.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query Having(string column, object value)
    {
        return Having(column, "=", value);
    }

    /// <summary>
    /// Adds a HAVING clause with the specified operator and value.
    /// </summary>
    /// <param name="column">The column to filter.</param>
    /// <param name="op">The comparison operator.</param>
    /// <param name="value">The value to compare against.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query Having(string column, string op, object value)
    {
        ValidateString(column, nameof(column));
        ValidateString(op, nameof(op));
        if (value == null)
        {
            throw new ArgumentException("Value cannot be null.", nameof(value));
        }
        _having.Add((column, op, value));
        return this;
    }

    /// <summary>
    /// Limits the number of rows returned by the query.
    /// </summary>
    /// <param name="limit">The maximum number of rows to return.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query Limit(int limit)
    {
        _limit = limit;
        _useTop = false;
        // Ensure pagination mode is exclusive
        // Limit/Offset mode should not use TOP
        return this;
    }

    /// <summary>
    /// Skips the specified number of rows before returning results.
    /// </summary>
    /// <param name="offset">The number of rows to skip.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query Offset(int offset)
    {
        _offset = offset;
        _useTop = false;
        return this;
    }

    /// <summary>
    /// Limits the number of rows using the TOP clause (SQL Server).
    /// </summary>
    /// <param name="top">The number of rows to return.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query Top(int top)
    {
        _limit = top;
        _useTop = true;
        // Reset offset when switching to TOP to avoid mixed pagination modes
        _offset = null;
        return this;
    }

    /// <summary>
    /// Adds a UNION clause combining the results of another query.
    /// </summary>
    /// <param name="query">The query to union with.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query Union(Query query)
    {
        if (query == null)
        {
            throw new ArgumentException("Query cannot be null.", nameof(query));
        }
        _compoundQueries.Add(("UNION", query));
        return this;
    }

    /// <summary>
    /// Adds a UNION ALL clause combining the results of another query.
    /// </summary>
    /// <param name="query">The query to union with.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query UnionAll(Query query)
    {
        if (query == null)
        {
            throw new ArgumentException("Query cannot be null.", nameof(query));
        }
        _compoundQueries.Add(("UNION ALL", query));
        return this;
    }

    /// <summary>
    /// Adds an INTERSECT clause combining the results of another query.
    /// </summary>
    /// <param name="query">The query to intersect with.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query Intersect(Query query)
    {
        if (query == null)
        {
            throw new ArgumentException("Query cannot be null.", nameof(query));
        }
        _compoundQueries.Add(("INTERSECT", query));
        return this;
    }

    /// <summary>
    /// Adds VALUES for an INSERT statement.
    /// </summary>
    /// <param name="values">The values to insert.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query Values(params object[] values)
    {
        if (values == null || values.Length == 0)
        {
            throw new ArgumentException("Values cannot be null or empty.", nameof(values));
        }
        foreach (var value in values)
        {
            if (value == null)
            {
                throw new ArgumentException("Values cannot contain null.", nameof(values));
            }
        }
        _values.AddRange(values);
        return this;
    }

    private static void ValidateString(string value, string paramName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException($"{paramName} cannot be null or empty.", paramName);
        }
    }

    private static void ValidateStrings(string[] values, string paramName)
    {
        if (values == null || values.Length == 0)
        {
            throw new ArgumentException($"{paramName} cannot be null or empty.", paramName);
        }

        foreach (var value in values)
        {
            ValidateString(value, paramName);
        }
    }

    /// <summary>
    /// Compiles the current query to a SQL string.
    /// </summary>
    /// <returns>The compiled SQL statement.</returns>
    public string Compile()
    {
        var compiler = new QueryCompiler();
        return compiler.Compile(this);
    }

    /// <summary>
    /// Compiles the current query and returns the SQL string and parameters separately.
    /// </summary>
    /// <returns>A tuple containing the SQL statement and parameter values.</returns>
    public (string Sql, IReadOnlyList<object> Parameters) CompileWithParameters()
    {
        var compiler = new QueryCompiler();
        return compiler.CompileWithParameters(this);
    }

    public IReadOnlyList<string> SelectColumns => _select;
    public string Table => _from;
    public (Query Query, string Alias)? FromSubquery => _fromSubquery;
    public IReadOnlyList<IWhereToken> WhereTokens => _where;
    public string InsertTable => _insertTable;
    public IReadOnlyList<string> InsertColumns => _insertColumns;
    public IReadOnlyList<object> InsertValues => _values;
    public string UpdateTable => _updateTable;
    public IReadOnlyList<(string Column, object Value)> SetValues => _set;
    public string DeleteTable => _deleteTable;
    public IReadOnlyList<string> OrderByColumns => _orderBy;
    public IReadOnlyList<string> GroupByColumns => _groupBy;
    public IReadOnlyList<(string Column, string Operator, object Value)> HavingClauses => _having;
    public IReadOnlyList<(string Type, string Table, string? Condition)> Joins => _joins;
    public IReadOnlyList<(string Type, Query Query)> CompoundQueries => _compoundQueries;
    public int? LimitValue => _limit;
    public int? OffsetValue => _offset;
    public bool UseTop => _useTop;
}

public interface IWhereToken { }

public sealed record ConditionToken(string Column, string Operator, object Value) : IWhereToken;

public sealed record OperatorToken(string Operator) : IWhereToken;

public sealed record GroupStartToken() : IWhereToken;

public sealed record GroupEndToken() : IWhereToken;

public sealed record NullToken(string Column) : IWhereToken;

public sealed record NotNullToken(string Column) : IWhereToken;

public sealed record InToken(string Column, IReadOnlyList<object> Values) : IWhereToken;

public sealed record NotInToken(string Column, IReadOnlyList<object> Values) : IWhereToken;

public sealed record BetweenToken(string Column, object Start, object End) : IWhereToken;

public sealed record NotBetweenToken(string Column, object Start, object End) : IWhereToken;

