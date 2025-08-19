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
        await sql.BeginTransactionAsync("SQL1", "master", true, IsolationLevel.Serializable, cancellationToken).ConfigureAwait(false);
        try
        {
            await sql.QueryAsync("SQL1", "master", true, "CREATE TABLE #temp(id int)", null, true, cancellationToken).ConfigureAwait(false);
            await sql.CommitAsync(cancellationToken).ConfigureAwait(false);
            Console.WriteLine("Committed");
        }
        catch
        {
            await sql.RollbackAsync(cancellationToken).ConfigureAwait(false);
            Console.WriteLine("Rolled back");
        }
    }
}
