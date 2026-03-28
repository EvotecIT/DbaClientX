using DBAClientX;
using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

public static class TransactionAsyncExample
{
    public static async Task RunAsync(CancellationToken cancellationToken = default)
    {
        using var sql = new SqlServer();
        await sql.RunInTransactionAsync("SQL1", "master", true, async (client, token) =>
        {
            await client.QueryAsync("SQL1", "master", true, "CREATE TABLE #temp(id int)", null, true, token).ConfigureAwait(false);
        }, IsolationLevel.Serializable, cancellationToken).ConfigureAwait(false);

        Console.WriteLine("Committed");
    }
}
