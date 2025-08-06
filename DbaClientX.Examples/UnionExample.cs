using DBAClientX.QueryBuilder;

public static class UnionExample
{
    public static void Run()
    {
        var query = new Query()
            .Select("id")
            .From("users1")
            .UnionAll(new Query().Select("id").From("users2"));

        Console.WriteLine(QueryBuilder.Compile(query));
    }
}
