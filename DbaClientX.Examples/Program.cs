using System.Threading.Tasks;

public class Program
{
    public static async Task Main(string[] args)
    {
        var example = args.Length > 0 ? args[0].ToLowerInvariant() : string.Empty;
        switch (example)
        {
            case "asyncquery":
                await QuerySqlServerAsyncExample.RunAsync().ConfigureAwait(false);
                break;
            case "pgasyncquery":
                await QueryPostgreSqlAsyncExample.RunAsync().ConfigureAwait(false);
                break;
            case "mysqlasyncquery":
                await QueryMySqlAsyncExample.RunAsync().ConfigureAwait(false);
                break;
            case "nestedquery":
                NestedQueryExample.Run();
                break;
            case "parallelqueries":
                await ParallelQueriesExample.RunAsync().ConfigureAwait(false);
                break;
            case "transaction":
                await TransactionExample.RunAsync().ConfigureAwait(false);
                break;
            case "mysqltransaction":
                TransactionMySqlExample.Run();
                break;
            case "pgtransaction":
                await TransactionPostgreSqlExample.RunAsync().ConfigureAwait(false);
                break;
            case "cancellation":
                await CancellationExample.RunAsync().ConfigureAwait(false);
                break;
            case "streamquery":
                await StreamQueryExample.RunAsync().ConfigureAwait(false);
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
            case "inferdbtype":
                InferDbTypeExample.Run();
                break;
            case "joins":
                JoinExample.Run();
                break;
            case "upsert":
                InsertOrUpdateExample.Run();
                break;
            default:
                Console.WriteLine("Available examples: asyncquery, pgasyncquery, mysqlasyncquery, parallelqueries, transaction, mysqltransaction, cancellation, nestedquery, streamquery, nonquery, orderby, nullconditions, parameterized, inferdbtype, joins, upsert");
                break;
        }
    }
}
