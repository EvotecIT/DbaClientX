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

        public override Task<object?> QueryAsync(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, SqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null, string? username = null, string? password = null)
        {
            _responses.TryGetValue(query, out var result);
            return Task.FromResult(result);
        }
    }

    private class MockPostgreSql : DBAClientX.PostgreSql
    {
        private readonly IDictionary<string, object?> _responses;
        private readonly TimeSpan _delay;

        public MockPostgreSql(IDictionary<string, object?> responses, TimeSpan? delay = null)
        {
            _responses = responses;
            _delay = delay ?? TimeSpan.Zero;
        }

        public override async Task<object?> QueryAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, NpgsqlTypes.NpgsqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            if (_delay > TimeSpan.Zero)
            {
                await Task.Delay(_delay, cancellationToken);
            }

            _responses.TryGetValue(query, out var result);
            return result;
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

        using var sqlServer = new MockSqlServer(mapping);
        var results = await sqlServer.RunQueriesInParallel(queries, "s", "db", true, CancellationToken.None);

        Assert.Equal(new object?[] { 1, 2, 3 }, results);
    }

    [Fact]
    public async Task PostgreSql_RunQueriesInParallel_ReturnsResultsInOrder()
    {
        var queries = new[] { "q1", "q2", "q3" };
        var mapping = new Dictionary<string, object?>
        {
            ["q1"] = 1,
            ["q2"] = 2,
            ["q3"] = 3
        };

        using var postgreSql = new MockPostgreSql(mapping, TimeSpan.FromMilliseconds(10));
        var results = await postgreSql.RunQueriesInParallel(queries, "h", "db", "u", "p", maxDegreeOfParallelism: 2);

        Assert.Equal(new object?[] { 1, 2, 3 }, results);
    }

    [Fact]
    public async Task PostgreSql_RunQueriesInParallel_PropagatesFailures()
    {
        using var postgreSql = new FailingPostgreSql();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            postgreSql.RunQueriesInParallel(new[] { "ok", "boom" }, "h", "db", "u", "p", maxDegreeOfParallelism: 2));
    }

    private class FailingPostgreSql : DBAClientX.PostgreSql
    {
        public override Task<object?> QueryAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, NpgsqlTypes.NpgsqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            if (query == "boom")
            {
                throw new InvalidOperationException("boom");
            }

            return Task.FromResult<object?>(query);
        }
    }
}
