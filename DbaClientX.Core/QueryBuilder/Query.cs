using System;
using System.Collections.Generic;
using System.Linq;

namespace DBAClientX.QueryBuilder;

/// <summary>
/// Provides a fluent interface for constructing SQL queries in a provider-agnostic manner.
/// </summary>
public class Query
{
    private readonly List<string> _select = new();
    private bool _distinct;
    private string? _from;
    private (Query Query, string Alias)? _fromSubquery;
    private readonly List<(string Type, string Table, string? Condition)> _joins = new();
    private readonly List<IWhereToken> _where = new();
    private string? _insertTable;
    private readonly List<string> _insertColumns = new();
    private readonly List<IReadOnlyList<object>> _values = new();
    private bool _isUpsert;
    private readonly List<string> _conflictColumns = new();
    private readonly List<string> _upsertUpdateOnly = new();
    private string? _updateTable;
    private readonly List<(string Column, object Value)> _set = new();
    private string? _deleteTable;
    private readonly List<string> _orderBy = new();
    private readonly List<string> _groupBy = new();
    private readonly List<(string Column, string Operator, object Value)> _having = new();
    private int? _limit;
    private int? _offset;
    private bool _useTop;
    private readonly List<(string Type, Query Query)> _compoundQueries = new();
    private int _openGroups;

    /// <summary>
    /// Adds one or more columns to the <c>SELECT</c> clause.
    /// </summary>
    /// <param name="columns">Column expressions to include.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query Select(params string[] columns)
    {
        ValidateStrings(columns, nameof(columns));
        _select.AddRange(columns);
        return this;
    }

    /// <summary>
    /// Marks the query as <c>SELECT DISTINCT</c>.
    /// </summary>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query Distinct()
    {
        _distinct = true;
        return this;
    }

    /// <summary>
    /// Specifies the table for the <c>FROM</c> clause.
    /// </summary>
    /// <param name="table">The table name or expression.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query From(string table)
    {
        ValidateString(table, nameof(table));
        _from = table;
        _fromSubquery = null;
        return this;
    }

    /// <summary>
    /// Specifies a subquery for the <c>FROM</c> clause using an alias.
    /// </summary>
    /// <param name="subQuery">The subquery to use as a data source.</param>
    /// <param name="alias">The alias applied to the subquery.</param>
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
    /// Adds an inner join to the query.
    /// </summary>
    /// <param name="table">The joined table.</param>
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
    /// Adds a left outer join to the query.
    /// </summary>
    /// <inheritdoc cref="Join(string, string)"/>
    public Query LeftJoin(string table, string condition)
    {
        ValidateString(table, nameof(table));
        ValidateString(condition, nameof(condition));
        _joins.Add(("LEFT JOIN", table, condition));
        return this;
    }

    /// <summary>
    /// Adds a right outer join to the query.
    /// </summary>
    /// <inheritdoc cref="Join(string, string)"/>
    public Query RightJoin(string table, string condition)
    {
        ValidateString(table, nameof(table));
        ValidateString(condition, nameof(condition));
        _joins.Add(("RIGHT JOIN", table, condition));
        return this;
    }

    /// <summary>
    /// Adds a cross join to the query.
    /// </summary>
    /// <param name="table">The joined table.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query CrossJoin(string table)
    {
        ValidateString(table, nameof(table));
        _joins.Add(("CROSS JOIN", table, null));
        return this;
    }

    /// <summary>
    /// Adds a full outer join to the query.
    /// </summary>
    /// <inheritdoc cref="Join(string, string)"/>
    public Query FullOuterJoin(string table, string condition)
    {
        ValidateString(table, nameof(table));
        ValidateString(condition, nameof(condition));
        _joins.Add(("FULL OUTER JOIN", table, condition));
        return this;
    }

    /// <inheritdoc cref="Where(string, string, object)"/>
    public Query Where(string column, object value)
    {
        return Where(column, "=", value);
    }

    /// <summary>
    /// Adds a predicate to the <c>WHERE</c> clause using the specified operator and value.
    /// </summary>
    /// <param name="column">The column or expression to evaluate.</param>
    /// <param name="op">The comparison operator.</param>
    /// <param name="value">The value to compare with.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query Where(string column, string op, object value)
    {
        return AddCondition(column, op, value);
    }

    /// <summary>
    /// Adds a predicate to the <c>WHERE</c> clause comparing to a subquery.
    /// </summary>
    /// <param name="column">The column or expression to evaluate.</param>
    /// <param name="op">The comparison operator.</param>
    /// <param name="subQuery">The subquery that provides the comparison value.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query Where(string column, string op, Query subQuery)
    {
        return AddCondition(column, op, subQuery);
    }

    /// <inheritdoc cref="OrWhere(string, string, object)"/>
    public Query OrWhere(string column, object value)
    {
        return OrWhere(column, "=", value);
    }

    /// <summary>
    /// Adds an <c>OR</c> predicate to the <c>WHERE</c> clause using the specified operator and value.
    /// </summary>
    /// <inheritdoc cref="Where(string, string, object)"/>
    public Query OrWhere(string column, string op, object value)
    {
        return AddCondition(column, op, value, "OR");
    }

    /// <summary>
    /// Adds an <c>OR</c> predicate that compares to a subquery.
    /// </summary>
    /// <inheritdoc cref="Where(string, string, Query)"/>
    public Query OrWhere(string column, string op, Query subQuery)
    {
        return AddCondition(column, op, subQuery, "OR");
    }

    /// <summary>
    /// Adds an <c>IN</c> predicate to the <c>WHERE</c> clause.
    /// </summary>
    /// <param name="column">The column or expression to evaluate.</param>
    /// <param name="values">The values to test for membership.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query WhereIn(string column, params object[] values)
    {
        return AddInCondition(column, values);
    }

    /// <summary>
    /// Adds an <c>IN</c> predicate comparing to a subquery.
    /// </summary>
    /// <param name="column">The column or expression to evaluate.</param>
    /// <param name="subQuery">The subquery producing the value set.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query WhereIn(string column, Query subQuery)
    {
        return AddCondition(column, "IN", subQuery);
    }

    /// <summary>
    /// Adds an <c>OR IN</c> predicate to the <c>WHERE</c> clause.
    /// </summary>
    /// <inheritdoc cref="WhereIn(string, object[])"/>
    public Query OrWhereIn(string column, params object[] values)
    {
        return AddInCondition(column, values, "OR");
    }

    /// <summary>
    /// Adds an <c>OR IN</c> predicate comparing to a subquery.
    /// </summary>
    /// <inheritdoc cref="WhereIn(string, Query)"/>
    public Query OrWhereIn(string column, Query subQuery)
    {
        return AddCondition(column, "IN", subQuery, "OR");
    }

    /// <summary>
    /// Adds a <c>NOT IN</c> predicate to the <c>WHERE</c> clause.
    /// </summary>
    /// <inheritdoc cref="WhereIn(string, object[])"/>
    public Query WhereNotIn(string column, params object[] values)
    {
        return AddInCondition(column, values, null, true);
    }

    /// <summary>
    /// Adds a <c>NOT IN</c> predicate comparing to a subquery.
    /// </summary>
    /// <inheritdoc cref="WhereIn(string, Query)"/>
    public Query WhereNotIn(string column, Query subQuery)
    {
        return AddCondition(column, "NOT IN", subQuery);
    }

    /// <summary>
    /// Adds an <c>OR NOT IN</c> predicate to the <c>WHERE</c> clause.
    /// </summary>
    /// <inheritdoc cref="WhereIn(string, object[])"/>
    public Query OrWhereNotIn(string column, params object[] values)
    {
        return AddInCondition(column, values, "OR", true);
    }

    /// <summary>
    /// Adds an <c>OR NOT IN</c> predicate comparing to a subquery.
    /// </summary>
    /// <inheritdoc cref="WhereIn(string, Query)"/>
    public Query OrWhereNotIn(string column, Query subQuery)
    {
        return AddCondition(column, "NOT IN", subQuery, "OR");
    }

    /// <summary>
    /// Adds a <c>BETWEEN</c> predicate to the <c>WHERE</c> clause.
    /// </summary>
    /// <param name="column">The column or expression to evaluate.</param>
    /// <param name="start">The inclusive start value.</param>
    /// <param name="end">The inclusive end value.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query WhereBetween(string column, object start, object end)
    {
        return AddBetweenCondition(column, start, end);
    }

    /// <summary>
    /// Adds an <c>OR BETWEEN</c> predicate to the <c>WHERE</c> clause.
    /// </summary>
    /// <inheritdoc cref="WhereBetween(string, object, object)"/>
    public Query OrWhereBetween(string column, object start, object end)
    {
        return AddBetweenCondition(column, start, end, "OR");
    }

    /// <summary>
    /// Adds a <c>NOT BETWEEN</c> predicate to the <c>WHERE</c> clause.
    /// </summary>
    /// <inheritdoc cref="WhereBetween(string, object, object)"/>
    public Query WhereNotBetween(string column, object start, object end)
    {
        return AddBetweenCondition(column, start, end, null, true);
    }

    /// <summary>
    /// Adds an <c>OR NOT BETWEEN</c> predicate to the <c>WHERE</c> clause.
    /// </summary>
    /// <inheritdoc cref="WhereBetween(string, object, object)"/>
    public Query OrWhereNotBetween(string column, object start, object end)
    {
        return AddBetweenCondition(column, start, end, "OR", true);
    }

    /// <summary>
    /// Adds an <c>IS NULL</c> predicate to the <c>WHERE</c> clause.
    /// </summary>
    /// <param name="column">The column or expression to evaluate.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query WhereNull(string column)
    {
        return AddNullCondition(column);
    }

    /// <summary>
    /// Adds an <c>OR IS NULL</c> predicate to the <c>WHERE</c> clause.
    /// </summary>
    /// <inheritdoc cref="WhereNull(string)"/>
    public Query OrWhereNull(string column)
    {
        return AddNullCondition(column, "OR");
    }

    /// <summary>
    /// Adds an <c>IS NOT NULL</c> predicate to the <c>WHERE</c> clause.
    /// </summary>
    /// <inheritdoc cref="WhereNull(string)"/>
    public Query WhereNotNull(string column)
    {
        return AddNotNullCondition(column);
    }

    /// <summary>
    /// Adds an <c>OR IS NOT NULL</c> predicate to the <c>WHERE</c> clause.
    /// </summary>
    /// <inheritdoc cref="WhereNull(string)"/>
    public Query OrWhereNotNull(string column)
    {
        return AddNotNullCondition(column, "OR");
    }

    /// <summary>
    /// Starts a grouped condition within the <c>WHERE</c> clause.
    /// </summary>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query BeginGroup()
    {
        AddDefaultAndIfRequired();
        _where.Add(new GroupStartToken());
        _openGroups++;
        return this;
    }

    /// <summary>
    /// Closes the most recent grouped condition within the <c>WHERE</c> clause.
    /// </summary>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query EndGroup()
    {
        if (_openGroups == 0)
        {
            throw new InvalidOperationException("EndGroup called without a matching BeginGroup.");
        }
        _where.Add(new GroupEndToken());
        _openGroups--;
        return this;
    }

    /// <summary>
    /// Inserts a literal <c>OR</c> operator into the <c>WHERE</c> clause.
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
    /// Configures the query for an <c>INSERT</c> statement targeting the specified table and columns.
    /// </summary>
    /// <param name="table">The table to insert into.</param>
    /// <param name="columns">The columns receiving values.</param>
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
    /// Configures an upsert (<c>INSERT</c>/<c>UPDATE</c>) statement targeting the specified table.
    /// </summary>
    /// <param name="table">The table to insert into.</param>
    /// <param name="values">Column/value pairs describing the insert payload.</param>
    /// <param name="conflictColumns">Columns used to detect conflicts.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query InsertOrUpdate(string table, IEnumerable<(string Column, object Value)> values, params string[] conflictColumns)
    {
        ValidateString(table, nameof(table));
        if (values == null)
        {
            throw new ArgumentException("Values cannot be null.", nameof(values));
        }
        var valueList = values.ToList();
        if (valueList.Count == 0)
        {
            throw new ArgumentException("Values cannot be empty.", nameof(values));
        }
        ValidateStrings(conflictColumns, nameof(conflictColumns));

        _insertTable = table;
        _insertColumns.Clear();
        _values.Clear();
        var row = new List<object>(valueList.Count);
        foreach (var (column, value) in valueList)
        {
            ValidateString(column, nameof(values));
            if (value == null)
            {
                throw new ArgumentException("Value cannot be null.", nameof(values));
            }
            _insertColumns.Add(column);
            row.Add(value);
        }
        _values.Add(row);
        _conflictColumns.Clear();
        _conflictColumns.AddRange(conflictColumns);
        _isUpsert = true;
        return this;
    }

    /// <summary>
    /// Limits the set of columns updated during an upsert to the provided list. If not set, all insert columns are updated.
    /// </summary>
    /// <param name="columns">The columns to update when a conflict occurs.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query UpsertUpdateOnly(params string[] columns)
    {
        ValidateStrings(columns, nameof(columns));
        _upsertUpdateOnly.Clear();
        _upsertUpdateOnly.AddRange(columns);
        return this;
    }

    /// <summary>
    /// Configures the query for an <c>UPDATE</c> statement targeting the specified table.
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
    /// Adds a column/value pair to the <c>SET</c> clause of an <c>UPDATE</c> statement.
    /// </summary>
    /// <param name="column">The column to update.</param>
    /// <param name="value">The new value.</param>
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
    /// Configures the query for a <c>DELETE</c> statement targeting the specified table.
    /// </summary>
    /// <param name="table">The table from which rows will be deleted.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query DeleteFrom(string table)
    {
        ValidateString(table, nameof(table));
        _deleteTable = table;
        return this;
    }

    /// <summary>
    /// Adds columns to the <c>ORDER BY</c> clause in ascending order.
    /// </summary>
    /// <param name="columns">The columns or expressions to order by.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query OrderBy(params string[] columns)
    {
        ValidateStrings(columns, nameof(columns));
        _orderBy.AddRange(columns);
        return this;
    }

    /// <summary>
    /// Adds columns to the <c>ORDER BY</c> clause in descending order.
    /// </summary>
    /// <inheritdoc cref="OrderBy(string[])"/>
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
    /// Adds raw expressions to the <c>ORDER BY</c> clause.
    /// </summary>
    /// <param name="expressions">Raw ordering expressions.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query OrderByRaw(params string[] expressions)
    {
        ValidateStrings(expressions, nameof(expressions));
        _orderBy.AddRange(expressions);
        return this;
    }

    /// <summary>
    /// Adds columns to the <c>GROUP BY</c> clause.
    /// </summary>
    /// <param name="columns">The columns or expressions used for grouping.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query GroupBy(params string[] columns)
    {
        ValidateStrings(columns, nameof(columns));
        _groupBy.AddRange(columns);
        return this;
    }

    /// <inheritdoc cref="Having(string, string, object)"/>
    public Query Having(string column, object value)
    {
        return Having(column, "=", value);
    }

    /// <summary>
    /// Adds a predicate to the <c>HAVING</c> clause.
    /// </summary>
    /// <param name="column">The aggregated column or expression.</param>
    /// <param name="op">The comparison operator.</param>
    /// <param name="value">The value to compare with.</param>
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
    /// Applies a <c>LIMIT</c> (or provider equivalent) clause to restrict result count.
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
    /// Applies an <c>OFFSET</c> clause to skip a number of rows.
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
    /// Applies a <c>TOP</c> clause (SQL Server style) to limit results.
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
    /// Adds a <c>UNION</c> compound query.
    /// </summary>
    /// <param name="query">The query to union.</param>
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
    /// Adds a <c>UNION ALL</c> compound query.
    /// </summary>
    /// <inheritdoc cref="Union(Query)"/>
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
    /// Adds an <c>INTERSECT</c> compound query.
    /// </summary>
    /// <inheritdoc cref="Union(Query)"/>
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
    /// Adds a row of values to an <c>INSERT</c> statement.
    /// </summary>
    /// <param name="values">The values corresponding to the configured columns.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query Values(params object[] values)
    {
        if (_insertColumns.Count == 0)
        {
            throw new InvalidOperationException("InsertInto must be called before Values.");
        }

        if (values == null || values.Length != _insertColumns.Count)
        {
            throw new InvalidOperationException($"Expected {_insertColumns.Count} values, but got {values?.Length ?? 0}.");
        }

        _values.Add(new List<object>(values));
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
    /// Compiles the query into SQL text using the specified dialect.
    /// </summary>
    /// <param name="dialect">The target SQL dialect.</param>
    /// <returns>The compiled SQL text.</returns>
    public string Compile(SqlDialect dialect = SqlDialect.SqlServer)
    {
        var compiler = new QueryCompiler(dialect);
        return compiler.Compile(this);
    }

    /// <summary>
    /// Compiles the query into SQL text and collects ordered parameter values for the specified dialect.
    /// </summary>
    /// <param name="dialect">The target SQL dialect.</param>
    /// <returns>A tuple containing SQL text and parameter values.</returns>
    public (string Sql, IReadOnlyList<object> Parameters) CompileWithParameters(SqlDialect dialect = SqlDialect.SqlServer)
    {
        var compiler = new QueryCompiler(dialect);
        return compiler.CompileWithParameters(this);
    }

    /// <summary>
    /// Gets the selected columns for the query.
    /// </summary>
    public IReadOnlyList<string> SelectColumns => _select;

    /// <summary>
    /// Gets the table used in the <c>FROM</c> clause, when not using a subquery.
    /// </summary>
    public string? Table => _from;

    /// <summary>
    /// Gets the subquery used in the <c>FROM</c> clause, if any.
    /// </summary>
    public (Query Query, string Alias)? FromSubquery => _fromSubquery;

    /// <summary>
    /// Gets the sequence of tokens representing the <c>WHERE</c> clause.
    /// </summary>
    public IReadOnlyList<IWhereToken> WhereTokens => _where;

    /// <summary>
    /// Gets the table targeted by an <c>INSERT</c> statement.
    /// </summary>
    public string? InsertTable => _insertTable;

    /// <summary>
    /// Gets the column list used for <c>INSERT</c> statements.
    /// </summary>
    public IReadOnlyList<string> InsertColumns => _insertColumns;

    /// <summary>
    /// Gets the rows of values used for <c>INSERT</c> statements.
    /// </summary>
    public IReadOnlyList<IReadOnlyList<object>> InsertValues => _values;

    /// <summary>
    /// Gets a value indicating whether the query is configured as an upsert.
    /// </summary>
    public bool IsUpsert => _isUpsert;

    /// <summary>
    /// Gets the columns used to detect conflicts during an upsert.
    /// </summary>
    public IReadOnlyList<string> ConflictColumns => _conflictColumns;

    /// <summary>
    /// Gets the subset of columns that should be updated during an upsert.
    /// </summary>
    public IReadOnlyList<string> UpsertUpdateOnlyColumns => _upsertUpdateOnly;

    /// <summary>
    /// Gets the table targeted by an <c>UPDATE</c> statement.
    /// </summary>
    public string? UpdateTable => _updateTable;

    /// <summary>
    /// Gets the column/value pairs configured for an <c>UPDATE</c> statement.
    /// </summary>
    public IReadOnlyList<(string Column, object Value)> SetValues => _set;

    /// <summary>
    /// Gets the table targeted by a <c>DELETE</c> statement.
    /// </summary>
    public string? DeleteTable => _deleteTable;

    /// <summary>
    /// Gets the column expressions used for ordering.
    /// </summary>
    public IReadOnlyList<string> OrderByColumns => _orderBy;

    /// <summary>
    /// Gets the column expressions used for grouping.
    /// </summary>
    public IReadOnlyList<string> GroupByColumns => _groupBy;

    /// <summary>
    /// Gets the predicates configured for the <c>HAVING</c> clause.
    /// </summary>
    public IReadOnlyList<(string Column, string Operator, object Value)> HavingClauses => _having;

    /// <summary>
    /// Gets the join definitions applied to the query.
    /// </summary>
    public IReadOnlyList<(string Type, string Table, string? Condition)> Joins => _joins;

    /// <summary>
    /// Gets the compound queries appended to the current query.
    /// </summary>
    public IReadOnlyList<(string Type, Query Query)> CompoundQueries => _compoundQueries;

    /// <summary>
    /// Gets a value indicating whether the query uses <c>DISTINCT</c>.
    /// </summary>
    public bool IsDistinct => _distinct;

    /// <summary>
    /// Gets the configured <c>LIMIT</c> value, if any.
    /// </summary>
    public int? LimitValue => _limit;

    /// <summary>
    /// Gets the configured <c>OFFSET</c> value, if any.
    /// </summary>
    public int? OffsetValue => _offset;

    /// <summary>
    /// Gets a value indicating whether the query should use <c>TOP</c> semantics.
    /// </summary>
    public bool UseTop => _useTop;

    /// <summary>
    /// Gets the number of open grouping tokens to validate balanced conditions.
    /// </summary>
    public int OpenGroups => _openGroups;
}

/// <summary>
/// Marker interface for <c>WHERE</c> clause tokens.
/// </summary>
public interface IWhereToken { }

/// <summary>
/// Represents a comparison predicate in the <c>WHERE</c> clause.
/// </summary>
/// <param name="Column">The column being compared.</param>
/// <param name="Operator">The comparison operator.</param>
/// <param name="Value">The comparison value.</param>
public sealed record ConditionToken(string Column, string Operator, object Value) : IWhereToken;

/// <summary>
/// Represents a logical operator token (e.g., AND/OR).
/// </summary>
/// <param name="Operator">The logical operator text.</param>
public sealed record OperatorToken(string Operator) : IWhereToken;

/// <summary>
/// Marks the start of a grouped condition.
/// </summary>
public sealed record GroupStartToken() : IWhereToken;

/// <summary>
/// Marks the end of a grouped condition.
/// </summary>
public sealed record GroupEndToken() : IWhereToken;

/// <summary>
/// Represents an <c>IS NULL</c> predicate.
/// </summary>
/// <param name="Column">The column evaluated for null.</param>
public sealed record NullToken(string Column) : IWhereToken;

/// <summary>
/// Represents an <c>IS NOT NULL</c> predicate.
/// </summary>
/// <param name="Column">The column evaluated for non-null values.</param>
public sealed record NotNullToken(string Column) : IWhereToken;

/// <summary>
/// Represents an <c>IN</c> predicate with literal values.
/// </summary>
/// <param name="Column">The column being compared.</param>
/// <param name="Values">The values to test.</param>
public sealed record InToken(string Column, IReadOnlyList<object> Values) : IWhereToken;

/// <summary>
/// Represents a <c>NOT IN</c> predicate with literal values.
/// </summary>
/// <param name="Column">The column being compared.</param>
/// <param name="Values">The values to exclude.</param>
public sealed record NotInToken(string Column, IReadOnlyList<object> Values) : IWhereToken;

/// <summary>
/// Represents a <c>BETWEEN</c> predicate.
/// </summary>
/// <param name="Column">The column being compared.</param>
/// <param name="Start">The inclusive start value.</param>
/// <param name="End">The inclusive end value.</param>
public sealed record BetweenToken(string Column, object Start, object End) : IWhereToken;

/// <summary>
/// Represents a <c>NOT BETWEEN</c> predicate.
/// </summary>
/// <param name="Column">The column being compared.</param>
/// <param name="Start">The inclusive start value.</param>
/// <param name="End">The inclusive end value.</param>
public sealed record NotBetweenToken(string Column, object Start, object End) : IWhereToken;

