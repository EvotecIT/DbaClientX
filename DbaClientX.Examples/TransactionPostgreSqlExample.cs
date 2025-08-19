using DBAClientX;
using System;
using System.Data;
using System.Threading.Tasks;

public static class TransactionPostgreSqlExample
{
    public static Task RunAsync()
    {
        using var pg = new PostgreSql();
        pg.BeginTransaction("localhost", "postgres", "user", "password", IsolationLevel.Serializable);
        try
        {
            pg.Query("localhost", "postgres", "user", "password", "CREATE TABLE temp(id int)", null, true);
            pg.Commit();
            Console.WriteLine("Committed");
        }
        catch
        {
            pg.Rollback();
            Console.WriteLine("Rolled back");
        }
        return Task.CompletedTask;
    }
}

