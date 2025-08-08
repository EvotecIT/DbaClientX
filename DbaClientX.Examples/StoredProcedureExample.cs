using DBAClientX;
using System.Data;
using System.Collections.Generic;
using System.Data.SqlClient;

public static class StoredProcedureExample
{
    public static void Run()
    {
        using var sqlServer = new SqlServer
        {
            ReturnType = ReturnType.DataTable,
        };

        var parameters = new List<SqlParameter>
        {
            new("@dbname", "master")
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
