using System.Threading.Tasks;

public class Program
{
    public static async Task Main(string[] args)
    {
        var example = args.Length > 0 ? args[0].ToLowerInvariant() : string.Empty;
        switch (example)
        {
            case "asyncquery":
                await QuerySqlServerAsyncExample.RunAsync();
                break;
            default:
                Console.WriteLine("Available examples: asyncquery");
                break;
        }
    }
}
