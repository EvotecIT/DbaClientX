#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
using System.Runtime.CompilerServices;

namespace DBAClientX;

/// <summary>
/// Helpers for consuming streamed query results with bounded memory.
/// </summary>
public static class DbaAsyncEnumerableExtensions
{
    /// <summary>
    /// Groups a streamed sequence into chunks of at most <paramref name="size"/> items.
    /// </summary>
    /// <typeparam name="T">Item type.</typeparam>
    /// <param name="source">The streamed sequence, for example <c>QueryStreamAsync&lt;T&gt;</c>.</param>
    /// <param name="size">Maximum number of items per chunk.</param>
    /// <param name="cancellationToken">
    /// Token checked after each item and passed to <paramref name="source"/> through <c>WithCancellation</c>. Provider
    /// streams observe the token given to <c>QueryStreamAsync</c>, so pass the same token there to cancel the database read.
    /// </param>
    /// <returns>Chunks in source order. Only the current chunk is held in memory; the last chunk may be smaller.</returns>
    /// <remarks>
    /// Use chunks to write batches (bulk inserts, dataset sidecar chunks, HTTP responses) while reading a large result.
    /// Each chunk is a new list, so consumers may keep it after the next chunk is produced.
    /// </remarks>
    public static IAsyncEnumerable<IReadOnlyList<T>> ChunkAsync<T>(this IAsyncEnumerable<T> source, int size, CancellationToken cancellationToken = default)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (size < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(size), size, "Chunk size must be at least 1.");
        }

        return Chunk(source, size, cancellationToken);

        static async IAsyncEnumerable<IReadOnlyList<T>> Chunk(IAsyncEnumerable<T> source, int size, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var chunk = new List<T>(Math.Min(size, 1024));
            await foreach (var item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                chunk.Add(item);
                cancellationToken.ThrowIfCancellationRequested();
                if (chunk.Count == size)
                {
                    yield return chunk;
                    chunk = new List<T>(Math.Min(size, 1024));
                }
            }

            if (chunk.Count > 0)
            {
                yield return chunk;
            }
        }
    }
}
#endif
