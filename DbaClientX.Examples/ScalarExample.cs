using DBAClientX;
using System.Threading.Tasks;

public static class ScalarExample
{
    public static async Task RunAsync()
    {
        using var sqlite = new SQLite();
        var value = sqlite.ExecuteScalar(":memory:", "SELECT 42");
        System.Console.WriteLine($"ExecuteScalar result: {value}");

        value = await sqlite.ExecuteScalarAsync(":memory:", "SELECT 42").ConfigureAwait(false);
        System.Console.WriteLine($"ExecuteScalarAsync result: {value}");
    }
}
