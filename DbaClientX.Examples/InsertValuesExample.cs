using DBAClientX.QueryBuilder;

public static class InsertValuesExample
{
    public static void Run()
    {
        var query = new Query()
            .InsertInto("users", "name", "age")
            .Values("Alice", 30)
            .Values("Bob", 40);

        Console.WriteLine(QueryBuilder.Compile(query, SqlDialect.PostgreSql));
    }
}
