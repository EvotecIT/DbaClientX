using DBAClientX.QueryBuilder;

public static class ParameterizedQueryExample
{
    public static void Run()
    {
        var query = new Query()
            .Select("*")
            .From("users")
            .Where("name", "Alice")
            .Where("age", ">", 30);

        var (sql, parameters) = QueryBuilder.CompileWithParameters(query);
        Console.WriteLine(sql);
        Console.WriteLine("Parameters: " + string.Join(", ", parameters));
    }
}
