using DBAClientX;
using System.Data;

public static class QuerySqlServerAsyncExample
{
    public static async Task RunAsync()
    {
        var sqlServer = new SqlServer
        {
            ReturnType = ReturnType.DataTable,
        };

        var result = await sqlServer.SqlQueryAsync("SQL1", "master", true, "SELECT TOP 1 * FROM sys.databases");

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
