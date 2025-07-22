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

    [Fact]
    public async Task RunQueriesInParallel_InvalidServer_ExecutesConcurrently()
    {
        var queries = Enumerable.Repeat("SELECT 1", 3).ToArray();
        var sqlServer = new DBAClientX.SqlServer();

        var sequential = Stopwatch.StartNew();
        foreach (var query in queries)
        {
            try
            {
                await sqlServer.SqlQueryAsync("invalid", "master", true, query);
            }
            catch (SqlException)
            {
            }
        }
        sequential.Stop();

        var parallel = Stopwatch.StartNew();
        await Assert.ThrowsAsync<SqlException>(async () =>
        {
            await sqlServer.RunQueriesInParallel(queries, "invalid", "master", true);
        });
        parallel.Stop();

        Assert.True(parallel.Elapsed < sequential.Elapsed);
    }
}
