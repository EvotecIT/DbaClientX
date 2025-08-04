using DBAClientX.QueryBuilder;

public static class NullConditionsExample
{
    public static void Run()
    {
        var query = new Query()
            .Select("*")
            .From("users")
            .WhereNull("deleted_at")
            .OrWhereNotNull("verified_at");

        Console.WriteLine(QueryBuilder.Compile(query));
    }
}
