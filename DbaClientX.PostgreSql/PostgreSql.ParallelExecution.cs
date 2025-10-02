using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DBAClientX;

public partial class PostgreSql
{
    /// <summary>
    /// Executes the provided queries in parallel, optionally throttled by a maximum degree of parallelism.
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

        SemaphoreSlim? throttler = null;
        if (maxDegreeOfParallelism.HasValue && maxDegreeOfParallelism.Value > 0)
        {
            throttler = new SemaphoreSlim(maxDegreeOfParallelism.Value);
        }

        var taskList = new List<Task<object?>>();
        foreach (var query in queries)
        {
            if (throttler != null)
            {
                await throttler.WaitAsync(cancellationToken).ConfigureAwait(false);
            }

            var task = QueryAsync(host, database, username, password, query, null, false, cancellationToken);
            if (throttler != null)
            {
                task = task.ContinueWith(
                    t =>
                    {
                        throttler.Release();
                        return t.Result;
                    },
                    cancellationToken,
                    TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
            }

            taskList.Add(task);
        }

        try
        {
            var results = await Task.WhenAll(taskList).ConfigureAwait(false);
            return results;
        }
        finally
        {
            throttler?.Dispose();
        }
    }
}
