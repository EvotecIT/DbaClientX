using DBAClientX;
using System;
using System.Data;
using System.Threading;

public static class QueryOracleExample
{
    public static async Task RunAsync()
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

        oracle.ReturnType = ReturnType.DataRow;
        await foreach (DataRow row in oracle.QueryStreamAsync("OracleServer", "ORCL", "user", "pass", "SELECT * FROM dual", cancellationToken: CancellationToken.None))
        {
            foreach (DataColumn col in row.Table.Columns)
            {
                Console.Write($"{row[col]}\t");
            }
            Console.WriteLine();
        }
    }
}
