using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DBAClientX;

public partial class Oracle
{
    /// <summary>
    /// Executes the provided queries in parallel, optionally throttled by a maximum degree of parallelism.
    /// </summary>
    public async Task<IReadOnlyList<object?>> RunQueriesInParallel(
        IEnumerable<string> queries,
        string host,
        string serviceName,
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

        var taskList = new List<Task<object?>>();
        foreach (var query in queries)
        {
            async Task<object?> ExecuteQueryAsync(string sql)
            {
                await throttler.WaitAsync(cancellationToken).ConfigureAwait(false);

                try
                {
                    return await QueryAsync(host, serviceName, username, password, sql, null, false, cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    throttler.Release();
                }
            }

            taskList.Add(ExecuteQueryAsync(query));
        }

        var results = await Task.WhenAll(taskList).ConfigureAwait(false);
        return results;
    }
}
