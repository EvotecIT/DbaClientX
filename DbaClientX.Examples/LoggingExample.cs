using System;

public static class LoggingExample
{
    public static void Run()
    {
        using var client = new DBAClientX.SQLite { LogAction = Console.WriteLine };
        client.ExecuteScalar(":memory:", "SELECT 1");
    }
}
