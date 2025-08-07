using DBAClientX;
using System;
using System.Data;
using System.Threading.Tasks;
using System.Threading;

public static class ParallelQueriesExample
{
    public static async Task RunAsync()
    {
        var queries = new[]
        {
            "SELECT TOP 1 * FROM sys.databases",
            "SELECT TOP 1 * FROM sys.objects",
            "SELECT TOP 1 * FROM sys.schemas"
        };

        using var sqlServer = new SqlServer
        {
            ReturnType = ReturnType.DataTable,
        };

        var results = await sqlServer.RunQueriesInParallel(queries, "SQL1", "master", true, CancellationToken.None);

        var index = 0;
        foreach (var result in results)
        {
            Console.WriteLine($"Result {index++} contains {((DataTable?)result)?.Rows.Count ?? 0} rows");
        }
    }
}
