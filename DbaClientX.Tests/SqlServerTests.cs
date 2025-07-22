using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace DbaClientX.Tests;

public class SqlServerTests
{
    [Fact]
    public async Task SqlQueryAsync_InvalidServer_ThrowsSqlException()
    {
        var sqlServer = new DBAClientX.SqlServer();
        await Assert.ThrowsAsync<SqlException>(async () =>
        {
            await sqlServer.SqlQueryAsync("invalid", "master", true, "SELECT 1");
        });
    }

    private class DelaySqlServer : DBAClientX.SqlServer
    {
        private readonly TimeSpan _delay;

        public DelaySqlServer(TimeSpan delay)
        {
            _delay = delay;
        }

        public override async Task<object?> SqlQueryAsync(string serverOrInstance, string database, bool integratedSecurity, string query)
        {
            await Task.Delay(_delay);
            return null;
        }
    }

    [Fact]
    public async Task RunQueriesInParallel_ExecutesConcurrently()
    {
        var queries = Enumerable.Repeat("SELECT 1", 3).ToArray();
        var sqlServer = new DelaySqlServer(TimeSpan.FromMilliseconds(200));

        var sequential = Stopwatch.StartNew();
        foreach (var query in queries)
        {
            await sqlServer.SqlQueryAsync("ignored", "ignored", true, query);
        }
        sequential.Stop();

        var parallel = Stopwatch.StartNew();
        await sqlServer.RunQueriesInParallel(queries, "ignored", "ignored", true);
        parallel.Stop();

        Assert.True(parallel.Elapsed < sequential.Elapsed);
    }
}
