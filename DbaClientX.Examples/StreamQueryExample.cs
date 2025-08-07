using DBAClientX;
using System.Data;
using System.Threading;

public static class StreamQueryExample
{
    public static async Task RunAsync()
    {
        using var sqlServer = new SqlServer
        {
            ReturnType = ReturnType.DataRow
        };

        await foreach (DataRow row in sqlServer.QueryStreamAsync("SQL1", "master", true, "SELECT TOP 5 * FROM sys.databases", cancellationToken: CancellationToken.None))
        {
            foreach (DataColumn col in row.Table.Columns)
            {
                Console.Write($"{row[col]}\t");
            }
            Console.WriteLine();
        }
    }
}
