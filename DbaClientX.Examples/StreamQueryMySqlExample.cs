using DBAClientX;
using System.Data;
using System.Threading;

public static class StreamQueryMySqlExample
{
    public static async Task RunAsync()
    {
        using var mySql = new MySql
        {
            ReturnType = ReturnType.DataRow
        };

        await foreach (DataRow row in mySql.QueryStreamAsync("MYSQL1", "mysql", "user", "password", "SELECT 1", cancellationToken: CancellationToken.None).ConfigureAwait(false))
        {
            foreach (DataColumn col in row.Table.Columns)
            {
                Console.Write($"{row[col]}\t");
            }
            Console.WriteLine();
        }
    }
}
