using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace DbaClientX.Tests;

public class ParallelQueriesTests
{
    private class MockSqlServer : DBAClientX.SqlServer
    {
        private readonly IDictionary<string, object?> _responses;

        public MockSqlServer(IDictionary<string, object?> responses)
        {
            _responses = responses;
        }

        public override Task<object?> SqlQueryAsync(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, SqlDbType>? parameterTypes = null, string? username = null, string? password = null)
        {
            _responses.TryGetValue(query, out var result);
            return Task.FromResult(result);
        }
    }

    [Fact]
    public async Task RunQueriesInParallel_ReturnsResultsInOrder()
    {
        var queries = new[] { "q1", "q2", "q3" };
        var mapping = new Dictionary<string, object?>
        {
            ["q1"] = 1,
            ["q2"] = 2,
            ["q3"] = 3
        };

        var sqlServer = new MockSqlServer(mapping);
        var results = await sqlServer.RunQueriesInParallel(queries, "s", "db", true, CancellationToken.None);

        Assert.Equal(new object?[] { 1, 2, 3 }, results);
    }
}
