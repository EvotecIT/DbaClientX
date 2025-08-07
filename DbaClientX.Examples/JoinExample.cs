using DBAClientX.QueryBuilder;

public static class JoinExample
{
    public static void Run()
    {
        var query = new Query()
            .Select("u.name", "o.total")
            .From("users u")
            .FullOuterJoin("orders o", "u.id = o.user_id")
            .CrossJoin("regions r");

        Console.WriteLine(QueryBuilder.Compile(query));
    }
}
