using DBAClientX;
using System.Threading.Tasks;

public static class PingExample
{
    public static async Task RunAsync()
    {
        using var sqlServer = new SqlServer();
        var result = sqlServer.Ping("SQL1", "master", true);
        System.Console.WriteLine($"Ping result: {result}");

        result = await sqlServer.PingAsync("SQL1", "master", true).ConfigureAwait(false);
        System.Console.WriteLine($"PingAsync result: {result}");
    }
}
