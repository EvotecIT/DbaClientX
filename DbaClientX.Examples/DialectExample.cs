using DBAClientX.QueryBuilder;

public static class DialectExample
{
    public static void Run()
    {
        var query = new Query()
            .Select("*")
            .From("users")
            .OrderBy("name")
            .Limit(5)
            .Offset(2);

        Console.WriteLine("PostgreSql: " + QueryBuilder.Compile(query, SqlDialect.PostgreSql));
        Console.WriteLine("MySql:      " + QueryBuilder.Compile(query, SqlDialect.MySql));
        Console.WriteLine("SQLite:     " + QueryBuilder.Compile(query, SqlDialect.SQLite));
        Console.WriteLine("SqlServer:  " + QueryBuilder.Compile(query, SqlDialect.SqlServer));
        Console.WriteLine("Oracle:     " + QueryBuilder.Compile(query, SqlDialect.Oracle));
    }
}
