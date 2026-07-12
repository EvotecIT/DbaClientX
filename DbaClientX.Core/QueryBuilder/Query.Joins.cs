using System;

namespace DBAClientX.QueryBuilder;

public partial class Query
{
    /// <summary>Adds a caller-authored inner join expression.</summary>
    /// <remarks>Use an identifier-based overload when any input is not trusted.</remarks>
    [Obsolete("Join(table, condition) treats both arguments as raw SQL. Use JoinRaw or an identifier-based Join overload.")]
    public Query Join(string table, string condition) => JoinRaw(table, condition);

    /// <summary>Adds an inner join using quoted table and column identifiers.</summary>
    public Query Join(string table, string leftColumn, string rightColumn)
        => AddJoin("JOIN", table, alias: null, leftColumn, "=", rightColumn);

    /// <summary>Adds an inner join using quoted table and column identifiers.</summary>
    public Query Join(string table, string leftColumn, string op, string rightColumn)
        => AddJoin("JOIN", table, alias: null, leftColumn, op, rightColumn);

    /// <summary>Adds an aliased inner join using quoted table, alias, and column identifiers.</summary>
    public Query Join(string table, string alias, string leftColumn, string op, string rightColumn)
        => AddJoin("JOIN", table, alias, leftColumn, op, rightColumn);

    /// <summary>Adds a caller-authored inner join expression.</summary>
    /// <remarks>Never pass untrusted input to this method.</remarks>
    public Query JoinRaw(string tableExpression, string condition)
        => AddRawJoin("JOIN", tableExpression, condition);

    /// <summary>Adds a caller-authored left join expression.</summary>
    /// <remarks>Use an identifier-based overload when any input is not trusted.</remarks>
    [Obsolete("LeftJoin(table, condition) treats both arguments as raw SQL. Use LeftJoinRaw or an identifier-based LeftJoin overload.")]
    public Query LeftJoin(string table, string condition) => LeftJoinRaw(table, condition);

    /// <summary>Adds a left join using quoted table and column identifiers.</summary>
    public Query LeftJoin(string table, string leftColumn, string op, string rightColumn)
        => AddJoin("LEFT JOIN", table, alias: null, leftColumn, op, rightColumn);

    /// <summary>Adds an aliased left join using quoted identifiers.</summary>
    public Query LeftJoin(string table, string alias, string leftColumn, string op, string rightColumn)
        => AddJoin("LEFT JOIN", table, alias, leftColumn, op, rightColumn);

    /// <summary>Adds a caller-authored left join expression.</summary>
    /// <remarks>Never pass untrusted input to this method.</remarks>
    public Query LeftJoinRaw(string tableExpression, string condition)
        => AddRawJoin("LEFT JOIN", tableExpression, condition);

    /// <summary>Adds a caller-authored right join expression.</summary>
    /// <remarks>Use an identifier-based overload when any input is not trusted.</remarks>
    [Obsolete("RightJoin(table, condition) treats both arguments as raw SQL. Use RightJoinRaw or an identifier-based RightJoin overload.")]
    public Query RightJoin(string table, string condition) => RightJoinRaw(table, condition);

    /// <summary>Adds a right join using quoted table and column identifiers.</summary>
    public Query RightJoin(string table, string leftColumn, string op, string rightColumn)
        => AddJoin("RIGHT JOIN", table, alias: null, leftColumn, op, rightColumn);

    /// <summary>Adds an aliased right join using quoted identifiers.</summary>
    public Query RightJoin(string table, string alias, string leftColumn, string op, string rightColumn)
        => AddJoin("RIGHT JOIN", table, alias, leftColumn, op, rightColumn);

    /// <summary>Adds a caller-authored right join expression.</summary>
    /// <remarks>Never pass untrusted input to this method.</remarks>
    public Query RightJoinRaw(string tableExpression, string condition)
        => AddRawJoin("RIGHT JOIN", tableExpression, condition);

    /// <summary>Adds a cross join using a quoted table identifier.</summary>
    public Query CrossJoin(string table)
    {
        ValidateString(table, nameof(table));
        _joins.Add(new QueryJoinClause("CROSS JOIN", new QueryExpression(table, IsRaw: false), null, null, null, null, null));
        return this;
    }

    /// <summary>Adds an aliased cross join using quoted identifiers.</summary>
    public Query CrossJoin(string table, string alias)
    {
        ValidateString(table, nameof(table));
        ValidateString(alias, nameof(alias));
        _joins.Add(new QueryJoinClause("CROSS JOIN", new QueryExpression(table, IsRaw: false), alias, null, null, null, null));
        return this;
    }

    /// <summary>Adds a caller-authored cross join expression.</summary>
    /// <remarks>Never pass untrusted input to this method.</remarks>
    public Query CrossJoinRaw(string tableExpression)
    {
        ValidateString(tableExpression, nameof(tableExpression));
        _joins.Add(new QueryJoinClause("CROSS JOIN", new QueryExpression(tableExpression, IsRaw: true), null, null, null, null, null));
        return this;
    }

    /// <summary>Adds a caller-authored full outer join expression.</summary>
    /// <remarks>Use an identifier-based overload when any input is not trusted.</remarks>
    [Obsolete("FullOuterJoin(table, condition) treats both arguments as raw SQL. Use FullOuterJoinRaw or an identifier-based FullOuterJoin overload.")]
    public Query FullOuterJoin(string table, string condition) => FullOuterJoinRaw(table, condition);

    /// <summary>Adds a full outer join using quoted table and column identifiers.</summary>
    public Query FullOuterJoin(string table, string leftColumn, string op, string rightColumn)
        => AddJoin("FULL OUTER JOIN", table, alias: null, leftColumn, op, rightColumn);

    /// <summary>Adds an aliased full outer join using quoted identifiers.</summary>
    public Query FullOuterJoin(string table, string alias, string leftColumn, string op, string rightColumn)
        => AddJoin("FULL OUTER JOIN", table, alias, leftColumn, op, rightColumn);

    /// <summary>Adds a caller-authored full outer join expression.</summary>
    /// <remarks>Never pass untrusted input to this method.</remarks>
    public Query FullOuterJoinRaw(string tableExpression, string condition)
        => AddRawJoin("FULL OUTER JOIN", tableExpression, condition);

    private Query AddJoin(string type, string table, string? alias, string leftColumn, string op, string rightColumn)
    {
        ValidateString(table, nameof(table));
        if (alias != null)
        {
            ValidateString(alias, nameof(alias));
        }
        ValidateString(leftColumn, nameof(leftColumn));
        ValidateString(rightColumn, nameof(rightColumn));
        var normalizedOperator = QueryComparisonOperator.Normalize(op, nameof(op));
        _joins.Add(new QueryJoinClause(
            type,
            new QueryExpression(table, IsRaw: false),
            alias,
            leftColumn,
            normalizedOperator,
            rightColumn,
            RawCondition: null));
        return this;
    }

    private Query AddRawJoin(string type, string tableExpression, string condition)
    {
        ValidateString(tableExpression, nameof(tableExpression));
        ValidateString(condition, nameof(condition));
        _joins.Add(new QueryJoinClause(
            type,
            new QueryExpression(tableExpression, IsRaw: true),
            Alias: null,
            LeftColumn: null,
            Operator: null,
            RightColumn: null,
            RawCondition: condition));
        return this;
    }
}
