using DBAClientX;
using System.Data;
using System.Collections.Generic;

public static class StoredProcedureExample
{
    public static void Run()
    {
        var sqlServer = new SqlServer
        {
            ReturnType = ReturnType.DataTable,
        };

        var parameters = new Dictionary<string, object?>
        {
            ["@dbname"] = "master"
        };

        var result = sqlServer.ExecuteStoredProcedure("SQL1", "master", true, "sp_helpdb", parameters);

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
