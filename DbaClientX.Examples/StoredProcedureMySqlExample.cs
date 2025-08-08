using DBAClientX;
using System.Collections.Generic;
using System.Data;

public static class StoredProcedureMySqlExample
{
    public static void Run()
    {
        using var mySql = new MySql
        {
            ReturnType = ReturnType.DataTable,
        };

        var parameters = new Dictionary<string, object?>
        {
            ["@id"] = 1
        };

        var result = mySql.ExecuteStoredProcedure("localhost", "mysql", "user", "password", "sp_test", parameters);

        if (result is DataTable table)
        {
            foreach (DataRow row in table.Rows)
            {
                foreach (DataColumn col in table.Columns)
                {
                    Console.Write($"{row[col]}\t");
                }
                Console.WriteLine();
            }
        }
    }
}
