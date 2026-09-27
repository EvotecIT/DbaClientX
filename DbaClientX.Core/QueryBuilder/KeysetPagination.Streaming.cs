#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

namespace DBAClientX.QueryBuilder;

public sealed partial class KeysetPagination
{
    /// <summary>Gets or initializes a comparison of key tuples in the database's configured sort order.</summary>
    /// <remarks>
    /// Return a positive value when the first tuple comes after the second. The comparison must include every key and
    /// respect each column's direction. Automatic streaming uses this to reject duplicate or backward rows. Without a
    /// callback, numeric, boolean and temporal keys are compared in their natural order, adjusted for column direction.
    /// Text, GUID, binary and provider-specific keys require a callback because their database ordering can differ from
    /// CLR ordering. Query creation and manual page materialization do not require this callback.
    /// </remarks>
    public Func<IReadOnlyList<object?>, IReadOnlyList<object?>, int>? CompareKeys { get; init; }

    /// <summary>
    /// Reads keyset pages one after another, starting after <paramref name="cursor"/>.
    /// </summary>
    /// <typeparam name="T">Row type produced by <paramref name="executePage"/>.</typeparam>
    /// <param name="source">A <c>SELECT</c> query without ordering or limits.</param>
    /// <param name="dialect">The dialect of the provider that runs the page queries.</param>
    /// <param name="executePage">
    /// Runs one page query and streams its rows, for example
    /// <c>(sql, parameters, ct) =&gt; sqlite.QueryStreamAsync(path, sql, map, parameters, cancellationToken: ct)</c>.
    /// </param>
    /// <param name="keySelector">Returns the key values of a row, in key column order.</param>
    /// <param name="cursor">A cursor to resume from, or <see langword="null"/> to start at the first page.</param>
    /// <param name="cancellationToken">Token passed to <paramref name="executePage"/> and checked between pages.</param>
    /// <returns>
    /// Pages of at most <see cref="PageSize"/> rows. Each page carries the cursor for the next one, so a consumer can stop
    /// and resume later. Memory is bounded by the page size. An empty result, or a cursor at the end of the data, yields
    /// one empty page.
    /// </returns>
    /// <remarks>
    /// Each page query reads <see cref="PageSize"/> + 1 rows and stops reading as soon as the extra row arrives. For SQL
    /// Server <c>datetime2</c> keys, run the page queries with <c>UseDateTime2ForDateTimeParameters</c> enabled.
    /// </remarks>
    public IAsyncEnumerable<QueryPage<T>> ReadPagesAsync<T>(
        Query source,
        SqlDialect dialect,
        Func<string, IDictionary<string, object?>, CancellationToken, IAsyncEnumerable<T>> executePage,
        Func<T, IReadOnlyList<object?>> keySelector,
        string? cursor = null,
        CancellationToken cancellationToken = default)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (executePage == null)
        {
            throw new ArgumentNullException(nameof(executePage));
        }

        if (keySelector == null)
        {
            throw new ArgumentNullException(nameof(keySelector));
        }

        return Read(source, dialect, executePage, keySelector, cursor, cancellationToken);
    }

    /// <summary>
    /// Streams every row across keyset pages, starting after <paramref name="cursor"/>.
    /// </summary>
    /// <typeparam name="T">Row type produced by <paramref name="executePage"/>.</typeparam>
    /// <param name="source">A <c>SELECT</c> query without ordering or limits.</param>
    /// <param name="dialect">The dialect of the provider that runs the page queries.</param>
    /// <param name="executePage">Runs one page query and streams its rows.</param>
    /// <param name="keySelector">Returns the key values of a row, in key column order.</param>
    /// <param name="cursor">A cursor to resume from, or <see langword="null"/> to start at the first page.</param>
    /// <param name="cancellationToken">Token passed to <paramref name="executePage"/> and checked between pages.</param>
    /// <returns>Rows in key order. Memory is bounded by the page size.</returns>
    /// <remarks>Use <see cref="ReadPagesAsync{T}"/> when the consumer needs cursors to resume after a stop.</remarks>
    public IAsyncEnumerable<T> StreamAsync<T>(
        Query source,
        SqlDialect dialect,
        Func<string, IDictionary<string, object?>, CancellationToken, IAsyncEnumerable<T>> executePage,
        Func<T, IReadOnlyList<object?>> keySelector,
        string? cursor = null,
        CancellationToken cancellationToken = default)
        => Flatten(ReadPagesAsync(source, dialect, executePage, keySelector, cursor, cancellationToken), cancellationToken);

    private static async IAsyncEnumerable<T> Flatten<T>(IAsyncEnumerable<QueryPage<T>> pages, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var page in pages.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            foreach (var item in page.Items)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return item;
            }
        }
    }

    private const string NotAdvancingMessage =
        "The page query returned rows at or before the cursor. Check that executePage passes the parameters and that key values round-trip exactly (for SQL Server datetime2 keys, enable UseDateTime2ForDateTimeParameters).";

    private int CompareOrderedKeys(object[] current, object[] previous)
    {
        if (CompareKeys != null)
        {
            return CompareKeys(current, previous);
        }

        for (var index = 0; index < _columns.Length; index++)
        {
            var left = current[index];
            var right = previous[index];
            if (left.GetType() != right.GetType()
                || left is not (long or int or byte or sbyte or short or ushort or uint or ulong or decimal or float or double
                    or bool or DateTime or DateTimeOffset or TimeSpan
#if NET6_0_OR_GREATER
                    or DateOnly or TimeOnly
#endif
                    ))
            {
                throw new InvalidOperationException("Configure CompareKeys with the database's ordering for text, GUID, binary or provider-specific keys.");
            }

            var comparison = ((IComparable)left).CompareTo(right);
            if (comparison != 0)
            {
                return _columns[index].Descending ? (comparison > 0 ? -1 : 1) : comparison;
            }
        }

        return 0;
    }

    private async IAsyncEnumerable<QueryPage<T>> Read<T>(
        Query source,
        SqlDialect dialect,
        Func<string, IDictionary<string, object?>, CancellationToken, IAsyncEnumerable<T>> executePage,
        Func<T, IReadOnlyList<object?>> keySelector,
        string? cursor,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        do
        {
            cancellationToken.ThrowIfCancellationRequested();
            var (sql, parameters) = CreatePageQuery(source, cursor).CompileWithNamedParameters(dialect);
            var previousKeys = cursor == null ? null : QueryPageCursor.DecodeKeyset(_columns, cursor, _signingKey);
            var rows = new List<T>(Math.Min(PageSize + 1, 1024));
            var pageRows = executePage(sql, parameters, cancellationToken)
                ?? throw new InvalidOperationException("executePage returned null.");
            await foreach (var row in pageRows.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                // Round-trip through the same codec as the query so declared and widened integer keys compare alike.
                var rowCursor = CreateCursor(keySelector(row));
                var currentKeys = QueryPageCursor.DecodeKeyset(_columns, rowCursor, _signingKey);
                if (previousKeys != null && CompareOrderedKeys(currentKeys, previousKeys) <= 0)
                {
                    throw new InvalidOperationException(NotAdvancingMessage);
                }

                previousKeys = currentKeys;

                rows.Add(row);
                if (rows.Count > PageSize)
                {
                    break;
                }
            }

            string? nextCursor = null;
            if (rows.Count > PageSize)
            {
                rows.RemoveAt(PageSize);
                nextCursor = CreateCursor(keySelector(rows[PageSize - 1]));
                if (string.Equals(nextCursor, cursor, StringComparison.Ordinal))
                {
                    throw new InvalidOperationException(NotAdvancingMessage);
                }
            }

            cursor = nextCursor;
            yield return new QueryPage<T>(rows, nextCursor);
        } while (cursor != null);
    }
}
#endif
