using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DBAClientX;

public partial class SQLite
{
    /// <summary>
    /// Executes a collection of SQL queries concurrently using independent connections.
    /// </summary>
    public async Task<IReadOnlyList<object?>> RunQueriesInParallel(
        IEnumerable<string> queries,
        string database,
        CancellationToken cancellationToken = default,
        int? maxDegreeOfParallelism = null)
    {
        if (queries == null)
        {
            throw new ArgumentNullException(nameof(queries));
        }

        var effectiveMaxDegreeOfParallelism = maxDegreeOfParallelism.HasValue && maxDegreeOfParallelism.Value > 0
            ? maxDegreeOfParallelism.Value
            : DefaultMaxParallelQueries;
        using var throttler = new SemaphoreSlim(effectiveMaxDegreeOfParallelism);

        var tasks = queries.Select(async q =>
        {
            await throttler.WaitAsync(cancellationToken).ConfigureAwait(false);

            try
            {
                return await QueryAsync(database, q, null, false, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                throttler.Release();
            }
        });

        var results = await Task.WhenAll(tasks).ConfigureAwait(false);
        return results;
    }
}
