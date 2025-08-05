using DBAClientX;
using System;
using System.Threading;
using System.Threading.Tasks;

public static class TransactionAsyncExample
{
    public static async Task RunAsync(CancellationToken cancellationToken = default)
    {
        var sql = new SqlServer();
        await sql.BeginTransactionAsync("SQL1", "master", true, cancellationToken);
        try
        {
            await sql.SqlQueryAsync("SQL1", "master", true, "CREATE TABLE #temp(id int)", null, true, cancellationToken);
            await sql.CommitAsync(cancellationToken);
            Console.WriteLine("Committed");
        }
        catch
        {
            await sql.RollbackAsync(cancellationToken);
            Console.WriteLine("Rolled back");
        }
    }
}
