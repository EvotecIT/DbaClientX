using DBAClientX;
using System.Data;
using System.Threading;

public static class QuerySqlServerAsyncExample
{
    public static async Task RunAsync()
    {
        using var sqlServer = new SqlServer
        {
            ReturnType = ReturnType.DataTable,
        };

        var result = await sqlServer.QueryAsync("SQL1", "master", true, "SELECT TOP 1 * FROM sys.databases", cancellationToken: CancellationToken.None).ConfigureAwait(false);

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
