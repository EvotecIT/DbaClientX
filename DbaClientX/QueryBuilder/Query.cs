using System.Collections.Generic;

namespace DBAClientX.QueryBuilder;

public class Query
{
    private readonly List<string> _select = new();
    private string _from;
    private readonly List<(string Column, string Operator, object Value)> _where = new();
    private string _insertTable;
    private readonly List<string> _insertColumns = new();
    private readonly List<object> _values = new();

    public Query Select(params string[] columns)
    {
        _select.AddRange(columns);
        return this;
    }

    public Query From(string table)
    {
        _from = table;
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

    public Query InsertInto(string table, params string[] columns)
    {
        _insertTable = table;
        _insertColumns.AddRange(columns);
        return this;
    }

    public Query Values(params object[] values)
    {
        _values.AddRange(values);
        return this;
    }

    public IReadOnlyList<string> SelectColumns => _select;
    public string Table => _from;
    public IReadOnlyList<(string Column, string Operator, object Value)> WhereClauses => _where;
    public string InsertTable => _insertTable;
    public IReadOnlyList<string> InsertColumns => _insertColumns;
    public IReadOnlyList<object> InsertValues => _values;
}

