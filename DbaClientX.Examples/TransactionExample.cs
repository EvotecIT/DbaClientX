using DBAClientX;
using System;
using System.Threading.Tasks;

public static class TransactionExample
{
    public static Task RunAsync()
    {
        var sql = new SqlServer();
        sql.BeginTransaction("SQL1", "master", true);
        try
        {
            sql.SqlQuery("SQL1", "master", true, "CREATE TABLE #temp(id int)", null, true);
            sql.Commit();
            Console.WriteLine("Committed");
        }
        catch
        {
            sql.Rollback();
            Console.WriteLine("Rolled back");
        }
        return Task.CompletedTask;
    }
}
