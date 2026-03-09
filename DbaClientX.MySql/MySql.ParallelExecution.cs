using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DBAClientX;

public partial class MySql
{
    /// <summary>
    /// Executes multiple queries concurrently against the same connection information.
    /// </summary>
    public async Task<IReadOnlyList<object?>> RunQueriesInParallel(
        IEnumerable<string> queries,
        string host,
        string database,
        string username,
        string password,
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
                return await QueryAsync(host, database, username, password, q, cancellationToken: cancellationToken).ConfigureAwait(false);
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
