using DBAClientX;
using System.Data;
using System.Threading;

public static class StreamQueryOracleExample
{
    public static async Task RunAsync()
    {
        using var oracle = new Oracle
        {
            ReturnType = ReturnType.DataRow
        };

        await foreach (DataRow row in oracle.QueryStreamAsync("OracleServer", "ORCL", "user", "pass", "SELECT 1 FROM dual", cancellationToken: CancellationToken.None).ConfigureAwait(false))
        {
            foreach (DataColumn col in row.Table.Columns)
            {
                Console.Write($"{row[col]}\t");
            }
            Console.WriteLine();
        }
    }
}
