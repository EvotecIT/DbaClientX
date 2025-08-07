using DBAClientX;
using System.Data;
using System.Threading;

public static class QueryMySqlAsyncExample
{
    public static async Task RunAsync()
    {
        using var mySql = new MySql
        {
            ReturnType = ReturnType.DataTable,
        };

        var result = await mySql.QueryAsync("MYSQL1", "mysql", "user", "password", "SELECT 1", cancellationToken: CancellationToken.None);

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
