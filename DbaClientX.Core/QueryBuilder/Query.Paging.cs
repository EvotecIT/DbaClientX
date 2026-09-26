using System;
using System.Collections.Generic;

namespace DBAClientX.QueryBuilder;

public partial class Query
{
    /// <summary>
    /// Creates a copy of this <c>SELECT</c> query that reads one keyset page.
    /// </summary>
    /// <param name="columns">Key columns, in sort order.</param>
    /// <param name="after">Key values of the last row of the previous page, or <see langword="null"/> for the first page.</param>
    /// <param name="fetch">Number of rows to fetch.</param>
    internal Query CreateKeysetPageQuery(IReadOnlyList<KeysetColumn> columns, IReadOnlyList<object>? after, int fetch)
    {
        ValidatePageSource(requireOrderBy: false);
        if (_orderBy.Count > 0)
        {
            throw new InvalidOperationException("Keyset paging sets ORDER BY from its key columns; remove ORDER BY from the source query.");
        }

        var page = CloneForPaging();
        if (after != null)
        {
            page.AddKeysetPredicate(columns, after);
        }

        foreach (var column in columns)
        {
            page._orderBy.Add(new QueryOrderExpression(column.Column, IsRaw: false, column.Descending));
        }

        return page.Limit(fetch);
    }

    /// <summary>
    /// Creates a copy of this ordered <c>SELECT</c> query that reads one offset page.
    /// </summary>
    internal Query CreateOffsetPageQuery(int offset, int fetch)
    {
        ValidatePageSource(requireOrderBy: true);
        return CloneForPaging().Limit(fetch).Offset(offset);
    }

    private void ValidatePageSource(bool requireOrderBy)
    {
        if (_insertTable != null || _updateTable != null || _deleteTable != null)
        {
            throw new InvalidOperationException("Paging requires a SELECT query.");
        }

        if (_from == null && _fromSubquery == null)
        {
            throw new InvalidOperationException("Paging requires a FROM clause.");
        }

        if (_limit.HasValue || _offset.HasValue || _useTop)
        {
            throw new InvalidOperationException("Paging sets LIMIT/OFFSET itself; remove Limit, Offset and Top from the source query.");
        }

        if (_compoundQueries.Count > 0)
        {
            throw new InvalidOperationException("Paging does not support UNION or INTERSECT queries; page a subquery instead.");
        }

        if (_openGroups > 0)
        {
            throw new InvalidOperationException("The source query has an unclosed condition group.");
        }

        if (requireOrderBy && _orderBy.Count == 0)
        {
            throw new InvalidOperationException("Offset paging requires ORDER BY on a unique column set so pages are stable.");
        }
    }

    /// <summary>
    /// Adds the seek predicate <c>(k0 &gt; v0) OR (k0 = v0 AND k1 &gt; v1) OR ...</c>, using <c>&lt;</c> for descending keys.
    /// </summary>
    /// <remarks>
    /// The expanded form works on every dialect and with mixed sort directions. For more than one key, a leading
    /// <c>k0 &gt;= v0</c> range lets engines seek on an index whose first column is <c>k0</c>. Existing conditions are grouped
    /// first so an <c>OR</c> in the source query cannot bypass the seek.
    /// </remarks>
    private void AddKeysetPredicate(IReadOnlyList<KeysetColumn> columns, IReadOnlyList<object> after)
    {
        if (_where.Count > 0)
        {
            _where.Insert(0, new GroupStartToken());
            _where.Add(new GroupEndToken());
        }

        if (columns.Count == 1)
        {
            Where(columns[0].Column, columns[0].Descending ? "<" : ">", after[0]);
            return;
        }

        Where(columns[0].Column, columns[0].Descending ? "<=" : ">=", after[0]);
        BeginGroup();
        for (var index = 0; index < columns.Count; index++)
        {
            if (index > 0)
            {
                Or();
            }

            BeginGroup();
            for (var previous = 0; previous < index; previous++)
            {
                Where(columns[previous].Column, "=", after[previous]);
            }

            Where(columns[index].Column, columns[index].Descending ? "<" : ">", after[index]);
            EndGroup();
        }

        EndGroup();
    }

    private Query CloneForPaging()
    {
        var clone = new Query
        {
            _distinct = _distinct,
            _from = _from,
            _fromAlias = _fromAlias,
            _fromSubquery = _fromSubquery
        };
        clone._select.AddRange(_select);
        clone._joins.AddRange(_joins);
        clone._where.AddRange(_where);
        clone._orderBy.AddRange(_orderBy);
        clone._groupBy.AddRange(_groupBy);
        clone._having.AddRange(_having);
        return clone;
    }
}
