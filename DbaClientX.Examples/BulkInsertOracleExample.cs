using DBAClientX;
using System;
using System.Collections.Generic;
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
            foreach (DataRow row in table.Rows)
            {
                var parameters = new Dictionary<string, object?>
                {
                    ["Id"] = row["Id"],
                    ["Name"] = row["Name"]
                };
                oracle.ExecuteNonQuery("OracleServer", "ORCL", "user", "pass", "INSERT INTO ExampleTable (Id, Name) VALUES (:Id, :Name)", parameters, useTransaction: true);
            }
            oracle.Commit();
            Console.WriteLine("Bulk insert executed.");
        }
        catch
        {
            oracle.Rollback();
            Console.WriteLine("Bulk insert failed.");
        }
    }
}
