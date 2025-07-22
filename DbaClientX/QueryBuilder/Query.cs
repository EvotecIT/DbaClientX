using System.Collections.Generic;

namespace DBAClientX.QueryBuilder;

public class Query
{
    private readonly List<string> _select = new();
    private string _from;
    private readonly List<(string Column, string Operator, object Value)> _where = new();

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

    public IReadOnlyList<string> SelectColumns => _select;
    public string Table => _from;
    public IReadOnlyList<(string Column, string Operator, object Value)> WhereClauses => _where;
}

