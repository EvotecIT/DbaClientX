using DBAClientX;
using System;
using System.Data;

public static class QueryOracleExample
{
    public static void Run()
    {
        var connectionString = DBAClientX.Oracle.BuildConnectionString("OracleServer", "ORCL", "user", "pass");
        Console.WriteLine(connectionString);

        using var oracle = new DBAClientX.Oracle
        {
            ReturnType = ReturnType.DataTable,
        };

        var result = oracle.Query("OracleServer", "ORCL", "user", "pass", "SELECT 1 FROM dual");
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
