using DBAClientX;
using System.Data;

public static class QuerySqliteExample
{
    public static void Run()
    {
        var sqlite = new SQLite
        {
            ReturnType = ReturnType.DataTable,
        };

        var result = sqlite.SqliteQuery("example.db", "SELECT 1");

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
