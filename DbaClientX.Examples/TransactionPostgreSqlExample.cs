using DBAClientX;
using System;
using System.Data;
using System.Threading.Tasks;

public static class TransactionPostgreSqlExample
{
    public static Task RunAsync()
    {
        using var pg = new PostgreSql();
        pg.RunInTransaction("localhost", "postgres", "user", "password", client =>
        {
            client.Query("localhost", "postgres", "user", "password", "CREATE TABLE temp(id int)", null, true);
        }, IsolationLevel.Serializable);

        Console.WriteLine("Committed");
        return Task.CompletedTask;
    }
}

