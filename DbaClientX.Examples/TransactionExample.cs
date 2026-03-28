using DBAClientX;
using System;
using System.Data;
using System.Threading.Tasks;

public static class TransactionExample
{
    public static Task RunAsync()
    {
        using var sql = new SqlServer();
        sql.RunInTransaction("SQL1", "master", true, client =>
        {
            client.Query("SQL1", "master", true, "CREATE TABLE #temp(id int)", null, true);
        }, IsolationLevel.Serializable);

        Console.WriteLine("Committed");
        return Task.CompletedTask;
    }
}
