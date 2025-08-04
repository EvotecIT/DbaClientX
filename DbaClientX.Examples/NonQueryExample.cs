using System;
using System.Threading.Tasks;

public static class NonQueryExample
{
    public static void Run()
    {
        var sqlServer = new DBAClientX.SqlServer();
        var affected = sqlServer.SqlQueryNonQuery("SQL1", "master", true, "CREATE TABLE #Example (Id INT)");
        Console.WriteLine($"Rows affected: {affected}");
    }
}
