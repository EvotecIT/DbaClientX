using System.Collections.Generic;

namespace DBAClientX.QueryBuilder;

public class Query
{
    private readonly List<string> _select = new();
    private string _from;
    private (Query Query, string Alias)? _fromSubquery;
    private readonly List<(string Column, string Operator, object Value)> _where = new();
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
    private bool _useTop;

    public Query Select(params string[] columns)
    {
        _select.AddRange(columns);
        return this;
    }

    public Query From(string table)
    {
        _from = table;
        _fromSubquery = null;
        return this;
    }

    public Query From(Query subQuery, string alias)
    {
        _from = null;
        _fromSubquery = (subQuery, alias);
        return this;
    }

    public Query Where(string column, object value)
    {
        return Where(column, "=", value);
    }

    public Query Where(string column, string op, object value)
    {
        _where.Add((column, op, value));
        return this;
    }

    public Query Where(string column, string op, Query subQuery)
    {
        _where.Add((column, op, subQuery));
        return this;
    }

    public Query InsertInto(string table, params string[] columns)
    {
        _insertTable = table;
        _insertColumns.AddRange(columns);
        return this;
    }

    public Query Update(string table)
    {
        _updateTable = table;
        return this;
    }

    public Query Set(string column, object value)
    {
        _set.Add((column, value));
        return this;
    }

    public Query DeleteFrom(string table)
    {
        _deleteTable = table;
        return this;
    }

    public Query OrderBy(params string[] columns)
    {
        _orderBy.AddRange(columns);
        return this;
    }

    public Query GroupBy(params string[] columns)
    {
        _groupBy.AddRange(columns);
        return this;
    }

    public Query Having(string column, object value)
    {
        return Having(column, "=", value);
    }

    public Query Having(string column, string op, object value)
    {
        _having.Add((column, op, value));
        return this;
    }

    public Query Limit(int limit)
    {
        _limit = limit;
        _useTop = false;
        return this;
    }

    public Query Top(int top)
    {
        _limit = top;
        _useTop = true;
        return this;
    }

    public Query Values(params object[] values)
    {
        _values.AddRange(values);
        return this;
    }

    public IReadOnlyList<string> SelectColumns => _select;
    public string Table => _from;
    public (Query Query, string Alias)? FromSubquery => _fromSubquery;
    public IReadOnlyList<(string Column, string Operator, object Value)> WhereClauses => _where;
    public string InsertTable => _insertTable;
    public IReadOnlyList<string> InsertColumns => _insertColumns;
    public IReadOnlyList<object> InsertValues => _values;
    public string UpdateTable => _updateTable;
    public IReadOnlyList<(string Column, object Value)> SetValues => _set;
    public string DeleteTable => _deleteTable;
    public IReadOnlyList<string> OrderByColumns => _orderBy;
    public IReadOnlyList<string> GroupByColumns => _groupBy;
    public IReadOnlyList<(string Column, string Operator, object Value)> HavingClauses => _having;
    public int? LimitValue => _limit;
    public bool UseTop => _useTop;
}

