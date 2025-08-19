using System;
using System.Data;
using DBAClientX;

public static class TransactionMySqlExample
{
    public static void Run()
    {
        using var mySql = new MySql();
        mySql.BeginTransaction("MYSQL1", "mysql", "user", "password", IsolationLevel.Serializable);
        try
        {
            mySql.ExecuteNonQuery("MYSQL1", "mysql", "user", "password", "CREATE TABLE Example (Id INT)", useTransaction: true);
            mySql.Commit();
            Console.WriteLine("Committed");
        }
        catch
        {
            mySql.Rollback();
            Console.WriteLine("Rolled back");
        }
    }
}
