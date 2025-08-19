using System;
using System.Threading;
using System.Threading.Tasks;
using DBAClientX;

public static class NonQueryMySqlAsyncExample
{
    public static async Task RunAsync()
    {
        using var mySql = new MySql();
        var affected = await mySql.ExecuteNonQueryAsync("MYSQL1", "mysql", "user", "password", "CREATE TABLE Example (Id INT)", cancellationToken: CancellationToken.None).ConfigureAwait(false);
        Console.WriteLine($"Rows affected: {affected}");
    }
}
