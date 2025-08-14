using DBAClientX.QueryBuilder;

public static class DistinctExample
{
    public static void Run()
    {
        var query = new Query()
            .Select("name")
            .Distinct()
            .From("users");

        Console.WriteLine(QueryBuilder.Compile(query));
    }
}
