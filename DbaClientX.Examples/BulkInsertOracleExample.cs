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

        oracle.BulkInsert("localhost", "ORCL", "user", "password", table, "ExampleTable", batchSize: 1000, bulkCopyTimeout: 60);
        Console.WriteLine("Bulk insert executed.");
    }
}
