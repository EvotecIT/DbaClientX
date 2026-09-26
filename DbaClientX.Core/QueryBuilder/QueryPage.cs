using System;
using System.Collections.Generic;

namespace DBAClientX.QueryBuilder;

/// <summary>
/// One page of query results and the cursor for the next page.
/// </summary>
/// <typeparam name="T">Row type.</typeparam>
public sealed class QueryPage<T>
{
    /// <summary>
    /// Initializes a new page.
    /// </summary>
    /// <param name="items">Rows on this page.</param>
    /// <param name="nextCursor">Opaque cursor for the next page, or <see langword="null"/> when this is the last page.</param>
    public QueryPage(IReadOnlyList<T> items, string? nextCursor)
    {
        Items = items ?? throw new ArgumentNullException(nameof(items));
        NextCursor = nextCursor;
    }

    /// <summary>Gets the rows on this page.</summary>
    public IReadOnlyList<T> Items { get; }

    /// <summary>
    /// Gets the opaque cursor for the next page, or <see langword="null"/> when this is the last page.
    /// Pass it back to the pagination helper that created it; cursors are safe to round-trip through URLs.
    /// </summary>
    public string? NextCursor { get; }

    /// <summary>Gets a value indicating whether another page exists.</summary>
    public bool HasMore => NextCursor != null;
}
