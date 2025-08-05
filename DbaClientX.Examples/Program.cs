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
            case "pgasyncquery":
                await QueryPostgreSqlAsyncExample.RunAsync();
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
            case "orderby":
                OrderByExample.Run();
                break;
            case "nonquery":
                NonQueryExample.Run();
                break;
            case "nullconditions":
                NullConditionsExample.Run();
                break;
            case "parameterized":
                ParameterizedQueryExample.Run();
                break;
            default:
                Console.WriteLine("Available examples: asyncquery, pgasyncquery, parallelqueries, transaction, cancellation, nestedquery, streamquery, nonquery, orderby, nullconditions, parameterized");
                break;
        }
    }
}
