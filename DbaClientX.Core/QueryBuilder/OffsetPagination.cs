using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace DBAClientX.QueryBuilder;

/// <summary>
/// Pages an ordered <c>SELECT</c> query with <c>LIMIT</c>/<c>OFFSET</c> (or the dialect equivalent).
/// </summary>
/// <remarks>
/// Offset paging supports jumping to any page but reads and discards every earlier row, and rows inserted or deleted
/// between requests shift page boundaries. Prefer <see cref="KeysetPagination"/> for large or changing tables. The
/// source query must have <c>ORDER BY</c> on a unique column set and must not set <c>LIMIT</c>, <c>OFFSET</c>,
/// <c>TOP</c> or compound operators.
/// <para>
/// Cursors are opaque but not tamper-proof: a client can request any offset, so cap the page index or offset you
/// accept from untrusted callers. The offset is rendered as a literal, so each page compiles to a distinct statement.
/// </para>
/// </remarks>
public sealed class OffsetPagination
{
    /// <summary>
    /// Initializes a new offset pagination.
    /// </summary>
    /// <param name="pageSize">Maximum number of rows per page.</param>
    public OffsetPagination(int pageSize)
    {
        if (pageSize < 1 || pageSize == int.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(pageSize), pageSize, "Page size must be between 1 and Int32.MaxValue - 1.");
        }

        PageSize = pageSize;
    }

    /// <summary>Gets the maximum number of rows per page.</summary>
    public int PageSize { get; }

    /// <summary>
    /// Creates the query for the page at <paramref name="cursor"/>. The source query is not modified.
    /// </summary>
    /// <param name="source">An ordered <c>SELECT</c> query without limits.</param>
    /// <param name="cursor">A cursor from <see cref="QueryPage{T}.NextCursor"/>, or <see langword="null"/> for the first page.</param>
    /// <returns>A query that skips earlier rows and fetches <see cref="PageSize"/> + 1 rows.</returns>
    /// <exception cref="ArgumentException">The cursor is malformed or was created by keyset paging.</exception>
    /// <exception cref="InvalidOperationException">The source query cannot be paged.</exception>
    public Query CreatePageQuery(Query source, string? cursor = null)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return source.CreateOffsetPageQuery(GetOffset(cursor), PageSize + 1);
    }

    /// <summary>
    /// Creates the query for a zero-based page number.
    /// </summary>
    /// <param name="source">An ordered <c>SELECT</c> query without limits.</param>
    /// <param name="pageIndex">Zero-based page number.</param>
    /// <returns>A query that skips earlier rows and fetches <see cref="PageSize"/> + 1 rows.</returns>
    public Query CreatePageQuery(Query source, int pageIndex)
    {
        if (pageIndex < 0 || (long)pageIndex * PageSize > int.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(pageIndex), pageIndex, "Page index is out of range.");
        }

        return CreatePageQuery(source, pageIndex == 0 ? null : QueryPageCursor.EncodeOffset(pageIndex * PageSize));
    }

    /// <summary>
    /// Creates a page from the rows returned by a page query.
    /// </summary>
    /// <typeparam name="T">Row type.</typeparam>
    /// <param name="rows">Rows returned by the page query, in order.</param>
    /// <param name="cursor">The cursor used to create the page query; <see langword="null"/> for the first page.</param>
    /// <returns>At most <see cref="PageSize"/> rows and a cursor when more rows exist.</returns>
    /// <exception cref="ArgumentException">The cursor is malformed.</exception>
    /// <exception cref="InvalidOperationException">The next page offset exceeds <see cref="int.MaxValue"/>.</exception>
    public QueryPage<T> CreatePage<T>(IReadOnlyList<T> rows, string? cursor)
    {
        if (rows == null)
        {
            throw new ArgumentNullException(nameof(rows));
        }

        if (rows.Count <= PageSize)
        {
            return new QueryPage<T>(rows, null);
        }

        var nextOffset = (long)GetOffset(cursor) + PageSize;
        if (nextOffset > int.MaxValue)
        {
            throw new InvalidOperationException("The next page offset exceeds Int32.MaxValue; use keyset paging for this result.");
        }

        return new QueryPage<T>(rows.Take(PageSize).ToArray(), QueryPageCursor.EncodeOffset((int)nextOffset));
    }

    /// <summary>
    /// Creates a page from a table returned by a page query.
    /// </summary>
    /// <param name="table">The page query result.</param>
    /// <param name="cursor">The cursor used to create the page query; <see langword="null"/> for the first page.</param>
    /// <returns>At most <see cref="PageSize"/> rows and a cursor when more rows exist.</returns>
    /// <exception cref="ArgumentException">The cursor is malformed.</exception>
    /// <exception cref="InvalidOperationException">The next page offset exceeds <see cref="int.MaxValue"/>.</exception>
    public QueryPage<DataRow> CreatePage(DataTable table, string? cursor)
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        return CreatePage(table.Rows.Cast<DataRow>().ToArray(), cursor);
    }

    private static int GetOffset(string? cursor)
        => cursor == null ? 0 : QueryPageCursor.DecodeOffset(cursor);
}
