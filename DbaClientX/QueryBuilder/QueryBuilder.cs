namespace DBAClientX.QueryBuilder;

public static class QueryBuilder
{
    public static Query Query() => new Query();

    public static string Compile(Query query)
    {
        var compiler = new QueryCompiler();
        return compiler.Compile(query);
    }
}

