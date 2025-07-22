using System.Data.SqlClient;
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
}
