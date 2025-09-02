using DBAClientX;
using System.Data;
using System.Threading;

public static class QueryOracleAsyncExample
{
    public static async Task RunAsync()
    {
        using var oracle = new DBAClientX.Oracle
        {
            ReturnType = ReturnType.DataTable,
        };

        var result = await oracle.QueryAsync("ORCL", "ORCL", "user", "pass", "SELECT 1 FROM dual", cancellationToken: CancellationToken.None).ConfigureAwait(false);

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
