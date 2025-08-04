using DBAClientX.QueryBuilder;

public static class OrderByExample
{
    public static void Run()
    {
        var query = new Query()
            .Select("*")
            .From("users")
            .OrderByDescending("age")
            .OrderByRaw("RAND()");

        Console.WriteLine(QueryBuilder.Compile(query));
    }
}
