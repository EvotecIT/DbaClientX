using DBAClientX.QueryBuilder;

public static class GroupExample
{
    public static void Run()
    {
        var query = new Query()
            .Select("*")
            .From("users")
            .BeginGroup()
                .Where("age", "<", 18)
                .OrWhere("age", ">", 60)
            .EndGroup()
            .Where("active", true);

        Console.WriteLine(QueryBuilder.Compile(query));
    }
}
