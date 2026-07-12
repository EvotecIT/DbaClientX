using System.Collections.Generic;
using System.Linq;

namespace DBAClientX.QueryBuilder;

public partial class Query
{
    /// <summary>Gets the selected columns for the query.</summary>
    public IReadOnlyList<string> SelectColumns => _select.Select(static expression => expression.Text).ToArray();

    internal IReadOnlyList<QueryExpression> SelectExpressions => _select;

    /// <summary>Gets the table used in the <c>FROM</c> clause, when not using a subquery.</summary>
    public string? Table => _from?.Text;

    internal QueryExpression? TableExpression => _from;

    internal string? TableAlias => _fromAlias;

    /// <summary>Gets the subquery used in the <c>FROM</c> clause, if any.</summary>
    public (Query Query, string Alias)? FromSubquery => _fromSubquery;

    /// <summary>Gets the sequence of tokens representing the <c>WHERE</c> clause.</summary>
    public IReadOnlyList<IWhereToken> WhereTokens => _where;

    /// <summary>Gets the table targeted by an <c>INSERT</c> statement.</summary>
    public string? InsertTable => _insertTable;

    /// <summary>Gets the column list used for <c>INSERT</c> statements.</summary>
    public IReadOnlyList<string> InsertColumns => _insertColumns;

    /// <summary>Gets the rows of values used for <c>INSERT</c> statements.</summary>
    public IReadOnlyList<IReadOnlyList<object>> InsertValues => _values;

    /// <summary>Gets a value indicating whether the query is configured as an upsert.</summary>
    public bool IsUpsert => _isUpsert;

    /// <summary>Gets the columns used to detect conflicts during an upsert.</summary>
    public IReadOnlyList<string> ConflictColumns => _conflictColumns;

    /// <summary>Gets the subset of columns that should be updated during an upsert.</summary>
    public IReadOnlyList<string> UpsertUpdateOnlyColumns => _upsertUpdateOnly;

    /// <summary>Gets the table targeted by an <c>UPDATE</c> statement.</summary>
    public string? UpdateTable => _updateTable;

    /// <summary>Gets the column/value pairs configured for an <c>UPDATE</c> statement.</summary>
    public IReadOnlyList<(string Column, object Value)> SetValues => _set;

    /// <summary>Gets the table targeted by a <c>DELETE</c> statement.</summary>
    public string? DeleteTable => _deleteTable;

    /// <summary>Gets the column expressions used for ordering.</summary>
    public IReadOnlyList<string> OrderByColumns => _orderBy
        .Select(static expression => expression.Text + (expression.Descending ? " DESC" : string.Empty))
        .ToArray();

    internal IReadOnlyList<QueryOrderExpression> OrderByExpressions => _orderBy;

    /// <summary>Gets the column expressions used for grouping.</summary>
    public IReadOnlyList<string> GroupByColumns => _groupBy.Select(static expression => expression.Text).ToArray();

    internal IReadOnlyList<QueryExpression> GroupByExpressions => _groupBy;

    /// <summary>Gets the predicates configured for the <c>HAVING</c> clause.</summary>
    public IReadOnlyList<(string Column, string Operator, object Value)> HavingClauses => _having
        .Select(static clause => (clause.Expression, clause.Operator, clause.Value))
        .ToArray();

    internal IReadOnlyList<QueryHavingClause> HavingExpressions => _having;

    /// <summary>Gets the join definitions applied to the query.</summary>
    public IReadOnlyList<(string Type, string Table, string? Condition)> Joins => _joins
        .Select(static join => (
            join.Type,
            join.Table.Text,
            join.RawCondition ?? (join.LeftColumn == null ? null : $"{join.LeftColumn} {join.Operator} {join.RightColumn}")))
        .ToArray();

    internal IReadOnlyList<QueryJoinClause> JoinClauses => _joins;

    /// <summary>Gets the compound queries appended to the current query.</summary>
    public IReadOnlyList<(string Type, Query Query)> CompoundQueries => _compoundQueries;

    /// <summary>Gets a value indicating whether the query uses <c>DISTINCT</c>.</summary>
    public bool IsDistinct => _distinct;

    /// <summary>Gets the configured <c>LIMIT</c> value, if any.</summary>
    public int? LimitValue => _limit;

    /// <summary>Gets the configured <c>OFFSET</c> value, if any.</summary>
    public int? OffsetValue => _offset;

    /// <summary>Gets a value indicating whether the query should use <c>TOP</c> semantics.</summary>
    public bool UseTop => _useTop;

    /// <summary>Gets the number of open grouping tokens used to validate balanced conditions.</summary>
    public int OpenGroups => _openGroups;
}
