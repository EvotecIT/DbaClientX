using System;
using System.Data;

public static class BulkInsertExample
{
    public static void Run()
    {
        using var sqlServer = new DBAClientX.SqlServer();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("Name", typeof(string));
        table.Rows.Add(1, "Example");

        sqlServer.BulkInsert("SQL1", "master", true, table, "dbo.ExampleTable", batchSize: 1000, bulkCopyTimeout: 60);
        Console.WriteLine("Bulk insert executed.");
    }
}
