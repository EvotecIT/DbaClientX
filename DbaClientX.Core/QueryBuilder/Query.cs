using System;
using System.Collections.Generic;
using System.Linq;

namespace DBAClientX.QueryBuilder;

/// <summary>
/// Provides a fluent interface for constructing SQL queries in a provider-agnostic manner.
/// </summary>
public partial class Query
{
    private readonly List<QueryExpression> _select = new();
    private bool _distinct;
    private QueryExpression? _from;
    private string? _fromAlias;
    private (Query Query, string Alias)? _fromSubquery;
    private readonly List<QueryJoinClause> _joins = new();
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
    private readonly List<QueryOrderExpression> _orderBy = new();
    private readonly List<QueryExpression> _groupBy = new();
    private readonly List<QueryHavingClause> _having = new();
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
        foreach (var column in columns)
        {
            _select.Add(new QueryExpression(column, IsRaw: false));
        }
        return this;
    }

    /// <summary>
    /// Adds one or more caller-authored SQL expressions to the <c>SELECT</c> clause.
    /// </summary>
    /// <param name="expressions">Trusted SQL expressions that are emitted without identifier quoting.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    /// <remarks>Never pass untrusted input to this method.</remarks>
    public Query SelectRaw(params string[] expressions)
    {
        ValidateStrings(expressions, nameof(expressions));
        foreach (var expression in expressions)
        {
            _select.Add(new QueryExpression(expression, IsRaw: true));
        }
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
        _from = new QueryExpression(table, IsRaw: false);
        _fromAlias = null;
        _fromSubquery = null;
        return this;
    }

    /// <summary>
    /// Specifies a table and alias for the <c>FROM</c> clause using identifier quoting for both values.
    /// </summary>
    public Query From(string table, string alias)
    {
        ValidateString(table, nameof(table));
        ValidateString(alias, nameof(alias));
        _from = new QueryExpression(table, IsRaw: false);
        _fromAlias = alias;
        _fromSubquery = null;
        return this;
    }

    /// <summary>
    /// Specifies a caller-authored SQL expression for the <c>FROM</c> clause.
    /// </summary>
    /// <remarks>Never pass untrusted input to this method.</remarks>
    public Query FromRaw(string expression)
    {
        ValidateString(expression, nameof(expression));
        _from = new QueryExpression(expression, IsRaw: true);
        _fromAlias = null;
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
        _fromAlias = null;
        _fromSubquery = (subQuery, alias);
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
    /// Adds a predicate whose left side is a caller-authored SQL expression.
    /// </summary>
    /// <remarks>Never pass untrusted input to <paramref name="expression"/>.</remarks>
    public Query WhereRaw(string expression, string op, object value)
    {
        return AddCondition(expression, op, value, isRawExpression: true);
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
    /// Adds an <c>OR</c> predicate whose left side is a caller-authored SQL expression.
    /// </summary>
    /// <remarks>Never pass untrusted input to <paramref name="expression"/>.</remarks>
    public Query OrWhereRaw(string expression, string op, object value)
    {
        return AddCondition(expression, op, value, "OR", isRawExpression: true);
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

    private Query AddCondition(string column, string op, object value, string? logical = null, bool isRawExpression = false)
    {
        ValidateString(column, nameof(column));
        var normalizedOperator = QueryComparisonOperator.Normalize(op, nameof(op));
        if (value == null)
        {
            throw new ArgumentException("Value cannot be null.", nameof(value));
        }
        if ((normalizedOperator == "IN" || normalizedOperator == "NOT IN") && value is not Query)
        {
            throw new ArgumentException("Use WhereIn/WhereNotIn for value lists; IN operators on this overload require a Query value.", nameof(value));
        }
        AddLogicalOperator(logical);
        _where.Add(isRawExpression
            ? new RawConditionToken(column, normalizedOperator, value)
            : new ConditionToken(column, normalizedOperator, value));
        return this;
    }

    private Query AddNullCondition(string column, string? logical = null, bool isRawExpression = false)
    {
        ValidateString(column, nameof(column));
        AddLogicalOperator(logical);
        _where.Add(isRawExpression ? new RawNullToken(column) : new NullToken(column));
        return this;
    }

    private Query AddNotNullCondition(string column, string? logical = null, bool isRawExpression = false)
    {
        ValidateString(column, nameof(column));
        AddLogicalOperator(logical);
        _where.Add(isRawExpression ? new RawNotNullToken(column) : new NotNullToken(column));
        return this;
    }

    private Query AddInCondition(string column, object[] values, string? logical = null, bool not = false, bool isRawExpression = false)
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
            _where.Add(isRawExpression ? new RawNotInToken(column, list) : new NotInToken(column, list));
        }
        else
        {
            _where.Add(isRawExpression ? new RawInToken(column, list) : new InToken(column, list));
        }
        return this;
    }

    private Query AddBetweenCondition(string column, object start, object end, string? logical = null, bool not = false, bool isRawExpression = false)
    {
        ValidateString(column, nameof(column));
        if (start == null || end == null)
        {
            throw new ArgumentException("Between values cannot be null.");
        }
        AddLogicalOperator(logical);
        if (not)
        {
            _where.Add(isRawExpression ? new RawNotBetweenToken(column, start, end) : new NotBetweenToken(column, start, end));
        }
        else
        {
            _where.Add(isRawExpression ? new RawBetweenToken(column, start, end) : new BetweenToken(column, start, end));
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

}

