using System;
using DBAClientX;
using System.Data;
using System.Threading;

public static class QueryPostgreSqlAsyncExample
{
    public static async Task RunAsync()
    {
        var pg = new PostgreSql
        {
            ReturnType = ReturnType.DataTable,
        };

        var result = await pg.PgQueryAsync("localhost", "postgres", "user", "password", "SELECT 1", cancellationToken: CancellationToken.None);

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
