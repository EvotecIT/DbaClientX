#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

namespace DBAClientX.QueryBuilder;

public sealed partial class KeysetPagination
{
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
        => Flatten(ReadPagesAsync(source, dialect, executePage, keySelector, cursor, cancellationToken));

    private static async IAsyncEnumerable<T> Flatten<T>(IAsyncEnumerable<QueryPage<T>> pages, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var page in pages.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            foreach (var item in page.Items)
            {
                yield return item;
            }
        }
    }

    private const string NotAdvancingMessage =
        "The page query returned rows at or before the cursor. Check that executePage passes the parameters and that key values round-trip exactly (for SQL Server datetime2 keys, enable UseDateTime2ForDateTimeParameters).";

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
            var rows = new List<T>(Math.Min(PageSize + 1, 1024));
            var pageRows = executePage(sql, parameters, cancellationToken)
                ?? throw new InvalidOperationException("executePage returned null.");
            await foreach (var row in pageRows.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                // Rows at or before the cursor mean the parameters were ignored or the key did not round-trip exactly.
                if (rows.Count == 0 && cursor != null && string.Equals(CreateCursor(keySelector(row)), cursor, StringComparison.Ordinal))
                {
                    throw new InvalidOperationException(NotAdvancingMessage);
                }

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
