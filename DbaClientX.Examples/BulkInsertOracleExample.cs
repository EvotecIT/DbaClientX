using System;
using System.Data;

public static class BulkInsertOracleExample
{
    public static void Run()
    {
        using var oracle = new DBAClientX.Oracle();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("Name", typeof(string));
        table.Rows.Add(1, "Example");

        oracle.BeginTransaction("OracleServer", "ORCL", "user", "pass");
        try
        {
            oracle.BulkInsert("OracleServer", "ORCL", "user", "pass", table, "ExampleTable", useTransaction: true, batchSize: 1000, bulkCopyTimeout: 60);
            oracle.Commit();
            Console.WriteLine("Bulk insert executed.");
        }
        catch
        {
            oracle.Rollback();
            throw;
        }
    }
}
