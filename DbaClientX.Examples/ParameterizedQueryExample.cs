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
        for (int i = 0; i < parameters.Count; i++)
        {
            Console.WriteLine($"@p{i} = {parameters[i]}");
        }
    }
}

