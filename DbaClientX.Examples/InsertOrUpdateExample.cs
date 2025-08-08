using System;
using DBAClientX.QueryBuilder;

public static class InsertOrUpdateExample
{
    public static void Run()
    {
        var values = new[] { ("id", (object)1), ("name", "Alice") };
        var query = new Query()
            .InsertOrUpdate("users", values, "id");

        Console.WriteLine(QueryBuilder.Compile(query, SqlDialect.PostgreSql));
        Console.WriteLine(QueryBuilder.Compile(query, SqlDialect.MySql));
        Console.WriteLine(QueryBuilder.Compile(query, SqlDialect.SqlServer));
        Console.WriteLine(QueryBuilder.Compile(query, SqlDialect.SQLite));
    }
}
