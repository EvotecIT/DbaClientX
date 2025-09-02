using System;
using System.Data;
using DBAClientX;

public static class TransactionOracleExample
{
    public static void Run()
    {
        using var oracle = new DBAClientX.Oracle();
        oracle.BeginTransaction("OracleServer", "ORCL", "user", "pass", IsolationLevel.Serializable);
        try
        {
            oracle.ExecuteNonQuery("OracleServer", "ORCL", "user", "pass", "CREATE TABLE Example (Id NUMBER)", useTransaction: true);
            oracle.Commit();
            Console.WriteLine("Committed");
        }
        catch
        {
            oracle.Rollback();
            Console.WriteLine("Rolled back");
        }
    }
}
