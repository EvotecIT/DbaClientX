using DBAClientX;
using System.Collections.Generic;
using System.Data;

public static class StoredProcedurePostgreSqlExample
{
    public static void Run()
    {
        var pg = new PostgreSql
        {
            ReturnType = ReturnType.DataTable,
        };

        var parameters = new Dictionary<string, object?>
        {
            ["@id"] = 1
        };

        var result = pg.ExecuteStoredProcedure("localhost", "postgres", "user", "password", "sp_test", parameters);

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
