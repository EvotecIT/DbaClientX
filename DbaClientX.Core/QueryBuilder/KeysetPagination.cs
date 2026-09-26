using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace DBAClientX.QueryBuilder;

/// <summary>
/// Pages a <c>SELECT</c> query by key values (seek paging) instead of row offsets.
/// </summary>
/// <remarks>
/// <para>
/// Each page query filters on the last key of the previous page, so reading page N costs the same as reading page 1
/// when an index covers the key columns in order. Offsets, by contrast, make the database read and discard every
/// earlier row.
/// </para>
/// <para>
/// Requirements: the key columns together must be unique and non-null (end with the primary key when the leading
/// columns can repeat), and the source query must not set <c>ORDER BY</c>, <c>LIMIT</c>, <c>OFFSET</c>, <c>TOP</c>
/// or compound operators. With <c>DISTINCT</c>, the key columns must also be selected. Page queries compile for every
/// <see cref="SqlDialect"/>; compile them with <see cref="Query.CompileWithParameters(SqlDialect)"/> so cursor values
/// are sent as parameters.
/// </para>
/// <para>
/// Cursors are opaque but not signed. Treat them as untrusted input: a client can change the key values, which only
/// changes where the page starts.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// var paging = new KeysetPagination(100, KeysetColumn.Desc("CreatedUtc"), KeysetColumn.Asc("Id"));
/// var pageQuery = paging.CreatePageQuery(new Query().From("Events").Where("Zone", zone), cursor);
/// var (sql, parameters) = pageQuery.CompileWithParameters(SqlDialect.SQLite);
/// // execute, then: var page = paging.CreatePage(table); page.NextCursor feeds the next request.
/// </code>
/// </example>
public sealed class KeysetPagination
{
    private readonly KeysetColumn[] _columns;

    /// <summary>
    /// Initializes a new keyset pagination.
    /// </summary>
    /// <param name="pageSize">Maximum number of rows per page.</param>
    /// <param name="columns">Key columns in sort order. Together they must identify a row uniquely.</param>
    public KeysetPagination(int pageSize, params KeysetColumn[] columns)
    {
        if (pageSize < 1 || pageSize == int.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(pageSize), pageSize, "Page size must be between 1 and Int32.MaxValue - 1.");
        }

        if (columns == null || columns.Length == 0 || columns.Any(static column => column == null))
        {
            throw new ArgumentException("At least one non-null key column is required.", nameof(columns));
        }

        PageSize = pageSize;
        _columns = (KeysetColumn[])columns.Clone();
    }

    /// <summary>Gets the maximum number of rows per page.</summary>
    public int PageSize { get; }

    /// <summary>Gets the key columns in sort order.</summary>
    public IReadOnlyList<KeysetColumn> Columns => Array.AsReadOnly(_columns);

    /// <summary>
    /// Creates the query for the page after <paramref name="cursor"/>. The source query is not modified.
    /// </summary>
    /// <param name="source">A <c>SELECT</c> query without ordering or limits.</param>
    /// <param name="cursor">A cursor from <see cref="QueryPage{T}.NextCursor"/>, or <see langword="null"/> for the first page.</param>
    /// <returns>A query ordered by the key columns that fetches <see cref="PageSize"/> + 1 rows, so the extra row signals another page.</returns>
    /// <exception cref="ArgumentException">The cursor is malformed or was created for a different key.</exception>
    /// <exception cref="InvalidOperationException">The source query cannot be paged.</exception>
    public Query CreatePageQuery(Query source, string? cursor = null)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        var after = cursor == null ? null : QueryPageCursor.DecodeKeyset(_columns, cursor);
        return source.CreateKeysetPageQuery(_columns, after, PageSize + 1);
    }

    /// <summary>
    /// Creates a page from the rows returned by a page query.
    /// </summary>
    /// <typeparam name="T">Row type.</typeparam>
    /// <param name="rows">Rows returned by the page query, in order.</param>
    /// <param name="keySelector">Returns the key values of a row, in key column order.</param>
    /// <returns>At most <see cref="PageSize"/> rows and a cursor when more rows exist.</returns>
    public QueryPage<T> CreatePage<T>(IReadOnlyList<T> rows, Func<T, IReadOnlyList<object?>> keySelector)
    {
        if (rows == null)
        {
            throw new ArgumentNullException(nameof(rows));
        }

        if (keySelector == null)
        {
            throw new ArgumentNullException(nameof(keySelector));
        }

        if (rows.Count <= PageSize)
        {
            return new QueryPage<T>(rows, null);
        }

        var items = rows.Take(PageSize).ToArray();
        return new QueryPage<T>(items, CreateCursor(keySelector(items[items.Length - 1])));
    }

    /// <summary>
    /// Creates a page from a table returned by a page query, reading keys from <see cref="KeysetColumn.ResultColumn"/>.
    /// </summary>
    /// <param name="table">The page query result.</param>
    /// <returns>At most <see cref="PageSize"/> rows and a cursor when more rows exist.</returns>
    public QueryPage<DataRow> CreatePage(DataTable table)
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        return CreatePage(table.Rows.Cast<DataRow>().ToArray(), row => _columns.Select(column => row[column.ResultColumn]).ToArray());
    }

    /// <summary>
    /// Creates a cursor that continues after a row with the given key values.
    /// </summary>
    /// <param name="keyValues">Key values in key column order.</param>
    /// <returns>An opaque, URL-safe cursor.</returns>
    /// <exception cref="ArgumentException">The number of key values does not match <see cref="Columns"/>.</exception>
    /// <exception cref="InvalidOperationException">A key value is null, of an unsupported type, or too large for a cursor.</exception>
    public string CreateCursor(IReadOnlyList<object?> keyValues)
    {
        if (keyValues == null || keyValues.Count != _columns.Length)
        {
            throw new ArgumentException($"Exactly {_columns.Length} key values are required.", nameof(keyValues));
        }

        return QueryPageCursor.EncodeKeyset(_columns, keyValues);
    }
}
