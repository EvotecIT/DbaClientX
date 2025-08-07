using DBAClientX;
using System;
using System.Threading;
using System.Threading.Tasks;

public static class CancellationExample
{
    public static async Task RunAsync()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        using var sqlServer = new SqlServer();
        try
        {
            await sqlServer.QueryAsync("SQL1", "master", true, "WAITFOR DELAY '00:00:05'", cancellationToken: cts.Token);
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Query was cancelled");
        }
    }
}
