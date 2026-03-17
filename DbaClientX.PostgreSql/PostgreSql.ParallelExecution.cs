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

        var validatedQueries = new List<string>();
        var queryIndex = 0;
        foreach (var query in queries)
        {
            if (string.IsNullOrWhiteSpace(query))
            {
                throw new ArgumentException($"Query at index {queryIndex} cannot be null or whitespace.", nameof(queries));
            }

            validatedQueries.Add(query);
            queryIndex++;
        }

        var effectiveMaxDegreeOfParallelism = maxDegreeOfParallelism.HasValue && maxDegreeOfParallelism.Value > 0
            ? maxDegreeOfParallelism.Value
            : DefaultMaxParallelQueries;
        using var throttler = new SemaphoreSlim(effectiveMaxDegreeOfParallelism);

        var taskList = new List<Task<object?>>();
        foreach (var query in validatedQueries)
        {
            async Task<object?> ExecuteQueryAsync(string sql)
            {
                await throttler.WaitAsync(cancellationToken).ConfigureAwait(false);

                try
                {
                    return await QueryAsync(host, database, username, password, sql, null, false, cancellationToken).ConfigureAwait(false);
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
