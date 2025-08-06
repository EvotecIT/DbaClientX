using DBAClientX.QueryBuilder;

public static class InAndBetweenExample
{
    public static void Run()
    {
        var query = new Query()
            .Select("*")
            .From("users")
            .WhereIn("id", 1, 2, 3)
            .OrWhereBetween("age", 18, 30);

        Console.WriteLine(QueryBuilder.Compile(query));
    }
}
