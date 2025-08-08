using System.Collections.Generic;
using System.Threading.Tasks;

namespace DbaClientX.Tests;

public class LoggingTests
{
    [Fact]
    public void LogAction_IsCalled_ForExecuteNonQuery()
    {
        var logs = new List<string>();
        using var client = new DBAClientX.SQLite { LogAction = logs.Add };
        client.ExecuteNonQuery(":memory:", "CREATE TABLE Test(Id INTEGER)");
        Assert.Contains("CREATE TABLE Test(Id INTEGER)", logs);
    }

    [Fact]
    public async Task LogAction_IsCalled_ForExecuteScalarAsync()
    {
        var logs = new List<string>();
        using var client = new DBAClientX.SQLite { LogAction = logs.Add };
        await client.ExecuteScalarAsync(":memory:", "SELECT 1");
        Assert.Contains("SELECT 1", logs);
    }
}
