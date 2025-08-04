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
            case "nestedquery":
                NestedQueryExample.Run();
                break;
            case "parallelqueries":
                await ParallelQueriesExample.RunAsync();
                break;
            case "transaction":
                await TransactionExample.RunAsync();
                break;
            case "cancellation":
                await CancellationExample.RunAsync();
                break;
            case "streamquery":
                await StreamQueryExample.RunAsync();
                break;
            case "nonquery":
                NonQueryExample.Run();
                break;
            default:
                Console.WriteLine("Available examples: asyncquery, parallelqueries, transaction, cancellation, nestedquery, streamquery, nonquery");
                break;
        }
    }
}
