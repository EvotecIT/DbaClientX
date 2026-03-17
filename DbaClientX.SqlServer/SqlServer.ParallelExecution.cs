using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DBAClientX;

public partial class SqlServer
{
    /// <summary>
    /// Executes multiple queries concurrently, optionally throttling the degree of parallelism.
    /// </summary>
    /// <param name="queries">Collection of SQL statements to execute.</param>
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="maxDegreeOfParallelism">Optional limit on the number of concurrent executions.</param>
    /// <returns>A list containing the result of each query in submission order.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="queries"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// Each query is executed via <see cref="QueryAsync"/>.
    /// </remarks>
    public async Task<IReadOnlyList<object?>> RunQueriesInParallel(
        IEnumerable<string> queries,
        string serverOrInstance,
        string database,
        bool integratedSecurity,
        CancellationToken cancellationToken = default,
        string? username = null,
        string? password = null,
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

        var tasks = validatedQueries.Select(async q =>
        {
            await throttler.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                return await QueryAsync(serverOrInstance, database, integratedSecurity, q, null, false, cancellationToken, username: username, password: password).ConfigureAwait(false);
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
