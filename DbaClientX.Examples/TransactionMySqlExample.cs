using System;
using System.Data;
using DBAClientX;

public static class TransactionMySqlExample
{
    public static void Run()
    {
        using var mySql = new MySql();
        mySql.RunInTransaction("MYSQL1", "mysql", "user", "password", client =>
        {
            client.ExecuteNonQuery("MYSQL1", "mysql", "user", "password", "CREATE TABLE Example (Id INT)", useTransaction: true);
        }, IsolationLevel.Serializable);

        Console.WriteLine("Committed");
    }
}
