using System;
using System.Data;

public static class BulkInsertPostgreSqlExample
{
    public static void Run()
    {
        using var postgre = new DBAClientX.PostgreSql();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("Name", typeof(string));
        table.Rows.Add(1, "Example");

        postgre.BulkInsert("localhost", "database", "user", "password", table, "ExampleTable", batchSize: 1000, bulkCopyTimeout: 60);
        Console.WriteLine("Bulk insert executed.");
    }
}
