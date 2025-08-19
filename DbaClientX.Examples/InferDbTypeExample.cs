using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using DBAClientX;

public static class InferDbTypeExample
{
    public static void Run()
    {
        using var sqlServer = new SqlServer();
        var parameters = new Dictionary<string, object?>
        {
            ["@offset"] = DateTimeOffset.UtcNow,
            ["@duration"] = TimeSpan.FromMinutes(1),
            ["@letter"] = 'A'
        };
        sqlServer.Query("SQL1", "master", true, "SELECT @offset, @duration, @letter", parameters);
    }
}
