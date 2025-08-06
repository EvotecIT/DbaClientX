using System;
using System.Collections.Generic;

namespace DBAClientX.QueryBuilder;

public class Query
{
    private readonly List<string> _select = new();
    private string _from;
    private (Query Query, string Alias)? _fromSubquery;
    private readonly List<(string Type, string Table, string Condition)> _joins = new();
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

    public Query Select(params string[] columns)
    {
        ValidateStrings(columns, nameof(columns));
        _select.AddRange(columns);
        return this;
    }

    public Query From(string table)
    {
        ValidateString(table, nameof(table));
        _from = table;
        _fromSubquery = null;
        return this;
    }

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

    public Query Join(string table, string condition)
    {
        ValidateString(table, nameof(table));
        ValidateString(condition, nameof(condition));
        _joins.Add(("JOIN", table, condition));
        return this;
    }

    public Query LeftJoin(string table, string condition)
    {
        ValidateString(table, nameof(table));
        ValidateString(condition, nameof(condition));
        _joins.Add(("LEFT JOIN", table, condition));
        return this;
    }

    public Query RightJoin(string table, string condition)
    {
        ValidateString(table, nameof(table));
        ValidateString(condition, nameof(condition));
        _joins.Add(("RIGHT JOIN", table, condition));
        return this;
    }

    public Query Where(string column, object value)
    {
        return Where(column, "=", value);
    }

    public Query Where(string column, string op, object value)
    {
        return AddCondition(column, op, value);
    }

    public Query Where(string column, string op, Query subQuery)
    {
        return AddCondition(column, op, subQuery);
    }

    public Query OrWhere(string column, object value)
    {
        return OrWhere(column, "=", value);
    }

    public Query OrWhere(string column, string op, object value)
    {
        return AddCondition(column, op, value, "OR");
    }

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

    public Query WhereNotBetween(string column, object start, object end)
    {
        return AddBetweenCondition(column, start, end, null, true);
    }

    public Query OrWhereNotBetween(string column, object start, object end)
    {
        return AddBetweenCondition(column, start, end, "OR", true);
    }

    public Query WhereNull(string column)
    {
        return AddNullCondition(column);
    }

    public Query OrWhereNull(string column)
    {
        return AddNullCondition(column, "OR");
    }

    public Query WhereNotNull(string column)
    {
        return AddNotNullCondition(column);
    }

    public Query OrWhereNotNull(string column)
    {
        return AddNotNullCondition(column, "OR");
    }

    public Query BeginGroup()
    {
        AddDefaultAndIfRequired();
        _where.Add(new GroupStartToken());
        return this;
    }

    public Query EndGroup()
    {
        _where.Add(new GroupEndToken());
        return this;
    }

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

    public Query InsertInto(string table, params string[] columns)
    {
        ValidateString(table, nameof(table));
        ValidateStrings(columns, nameof(columns));
        _insertTable = table;
        _insertColumns.AddRange(columns);
        return this;
    }

    public Query Update(string table)
    {
        ValidateString(table, nameof(table));
        _updateTable = table;
        return this;
    }

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

    public Query DeleteFrom(string table)
    {
        ValidateString(table, nameof(table));
        _deleteTable = table;
        return this;
    }

    public Query OrderBy(params string[] columns)
    {
        ValidateStrings(columns, nameof(columns));
        _orderBy.AddRange(columns);
        return this;
    }

    public Query OrderByDescending(params string[] columns)
    {
        ValidateStrings(columns, nameof(columns));
        foreach (var column in columns)
        {
            _orderBy.Add($"{column} DESC");
        }
        return this;
    }

    public Query OrderByRaw(params string[] expressions)
    {
        ValidateStrings(expressions, nameof(expressions));
        _orderBy.AddRange(expressions);
        return this;
    }

    public Query GroupBy(params string[] columns)
    {
        ValidateStrings(columns, nameof(columns));
        _groupBy.AddRange(columns);
        return this;
    }

    public Query Having(string column, object value)
    {
        return Having(column, "=", value);
    }

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

    public Query Limit(int limit)
    {
        _limit = limit;
        _useTop = false;
        // Ensure pagination mode is exclusive
        // Limit/Offset mode should not use TOP
        return this;
    }

    public Query Offset(int offset)
    {
        _offset = offset;
        _useTop = false;
        return this;
    }

    public Query Top(int top)
    {
        _limit = top;
        _useTop = true;
        // Reset offset when switching to TOP to avoid mixed pagination modes
        _offset = null;
        return this;
    }

    public Query Union(Query query)
    {
        if (query == null)
        {
            throw new ArgumentException("Query cannot be null.", nameof(query));
        }
        _compoundQueries.Add(("UNION", query));
        return this;
    }

    public Query UnionAll(Query query)
    {
        if (query == null)
        {
            throw new ArgumentException("Query cannot be null.", nameof(query));
        }
        _compoundQueries.Add(("UNION ALL", query));
        return this;
    }

    public Query Intersect(Query query)
    {
        if (query == null)
        {
            throw new ArgumentException("Query cannot be null.", nameof(query));
        }
        _compoundQueries.Add(("INTERSECT", query));
        return this;
    }

    public Query Values(params object[] values)
    {
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

    public string Compile()
    {
        var compiler = new QueryCompiler();
        return compiler.Compile(this);
    }

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
    public IReadOnlyList<(string Type, string Table, string Condition)> Joins => _joins;
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

