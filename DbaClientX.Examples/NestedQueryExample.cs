using DBAClientX.QueryBuilder;

public static class NestedQueryExample
{
    public static void Run()
    {
        var subQuery = new Query()
            .Select("id")
            .From("admins");

        var query = new Query()
            .Select("*")
            .From("users")
            .Where("id", "IN", subQuery);

        Console.WriteLine(QueryBuilder.Compile(query));

        var groupQuery = new Query()
            .Select("age", "COUNT(*)")
            .From("users")
            .GroupBy("age")
            .Having("COUNT(*)", ">", 1);

        Console.WriteLine(QueryBuilder.Compile(groupQuery));
    }
}
