using System;
using System.Threading.Tasks;
using DBAClientX;

public static class RetryExample
{
    public static async Task RunAsync()
    {
        using var sqlite = new SQLite { MaxRetryAttempts = 3, RetryDelay = TimeSpan.FromSeconds(1) };
        try
        {
            var result = await sqlite.ExecuteScalarAsync("test.db", "SELECT 1").ConfigureAwait(false);
            Console.WriteLine($"Result: {result}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Operation failed after retries: {ex.Message}");
        }
    }
}
